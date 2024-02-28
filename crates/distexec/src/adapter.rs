use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    Distribution,
    ExecutionPlan,
    Partitioning,
    RecordBatchStream,
    SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use super::pipeline::{Sink, Source};
use super::Result;

pub struct SplicedPlan {
    /// The execution plan after splicing.
    proxied: Arc<dyn ExecutionPlan>,
    /// Inputs into the proxied plan. (child -> partition)
    inputs: Vec<Vec<Arc<Mutex<InputPartition>>>>,
    /// Stored context for delayed execution.
    ///
    /// We don't execute all partitions for the plan as soon as we get it as we
    /// may (eventually) execute different partitions on separate nodes.
    context: Arc<TaskContext>,
}

impl SplicedPlan {
    pub fn new_from_plan(
        plan: Arc<dyn ExecutionPlan>,
        depth: usize,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        // The point in the plan at which to splice the plan graph
        let mut splice_point = plan;
        let mut parent_plans = Vec::with_capacity(depth.saturating_sub(1));
        for _ in 0..depth {
            let children = splice_point.children();
            assert_eq!(
                children.len(),
                1,
                "can only group through nodes with a single child"
            );
            parent_plans.push(splice_point);
            splice_point = children.into_iter().next().unwrap();
        }

        // The children to replace with `ProxyExecutionPlan`
        let children = splice_point.children();
        let mut inputs = Vec::with_capacity(children.len());

        // The spliced plan with its children replaced with `ProxyExecutionPlan`
        let spliced = if !children.is_empty() {
            let mut proxies: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());

            for child in children {
                let count = child.output_partitioning().partition_count();

                let mut child_inputs = Vec::with_capacity(count);
                for _ in 0..count {
                    child_inputs.push(Default::default())
                }

                inputs.push(child_inputs.clone());
                proxies.push(Arc::new(ProxyExecutionPlan {
                    inner: child,
                    inputs: child_inputs,
                }));
            }

            splice_point.with_new_children(proxies)?
        } else {
            splice_point.clone()
        };

        // Reconstruct the parent graph
        let mut proxied = spliced;
        for parent in parent_plans.into_iter().rev() {
            proxied = parent.with_new_children(vec![proxied])?
        }

        Ok(SplicedPlan {
            proxied,
            inputs,
            context,
        })
    }
}

/// Adapater pipeline around an execution plan.
pub struct AdapterPipeline {
    plan: SplicedPlan,
    outputs: Vec<Mutex<OutputState>>,
}

impl AdapterPipeline {
    pub fn new(plan: SplicedPlan) -> Self {
        let partitions = plan.proxied.output_partitioning().partition_count();
        let outputs: Vec<_> = (0..partitions)
            .map(|_| Mutex::new(OutputState::NeedsExecute))
            .collect();
        AdapterPipeline { plan, outputs }
    }
}

impl fmt::Debug for AdapterPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AdapterPipeline").finish()
    }
}

/// State of the underlying execution plan.
enum OutputState {
    NeedsExecute,
    Executing { stream: SendableRecordBatchStream },
}

impl OutputState {
    /// Get the output stream for a partition.
    ///
    /// If the stream hasn't been created yet, a new one will be created using
    /// the given `plan`.
    fn output_stream_for_partition(
        &mut self,
        partition: usize,
        plan: &SplicedPlan,
    ) -> Result<&mut SendableRecordBatchStream> {
        match self {
            OutputState::NeedsExecute => {
                let stream = plan.proxied.execute(partition, plan.context.clone())?;
                *self = OutputState::Executing { stream };
                match self {
                    Self::Executing { stream } => Ok(stream),
                    _ => unreachable!(),
                }
            }
            OutputState::Executing { stream } => Ok(stream),
        }
    }
}

impl Sink for AdapterPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        let mut partition = self.plan.inputs[child][partition].lock();
        assert!(
            !partition.is_closed,
            "attempted to push to finished partition",
        );

        partition.buffer.push_back(input);
        if let Some(waker) = partition.waker.take() {
            waker.wake()
        }
        Ok(())
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        let mut partition = self.plan.inputs[child][partition].lock();
        assert!(
            !partition.is_closed,
            "attempted to finish partition more than once",
        );

        partition.is_closed = true;
        if let Some(waker) = partition.waker.take() {
            waker.wake()
        }

        Ok(())
    }
}

impl Source for AdapterPipeline {
    fn output_partitions(&self) -> usize {
        self.outputs.len()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut output = self.outputs[partition].lock();
        let stream = match output.output_stream_for_partition(partition, &self.plan) {
            Ok(stream) => stream,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        stream
            .poll_next_unpin(cx)
            .map(|opt| opt.map(|r| r.map_err(Into::into)))
    }
}

#[derive(Debug, Default)]
struct InputPartition {
    buffer: VecDeque<RecordBatch>,
    waker: Option<Waker>,
    is_closed: bool,
}

struct InputPartitionStream {
    schema: SchemaRef,
    partition: Arc<Mutex<InputPartition>>,
}

impl Stream for InputPartitionStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut partition = self.partition.lock();
        match partition.buffer.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if partition.is_closed => Poll::Ready(None),
            _ => {
                partition.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for InputPartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// This is a hack that allows injecting `InputPartitionStream` in place of the
/// streams yielded by the child of the wrapped `ExecutionPlan`.
#[derive(Debug)]
struct ProxyExecutionPlan {
    inner: Arc<dyn ExecutionPlan>,
    inputs: Vec<Arc<Mutex<InputPartition>>>,
}

impl ExecutionPlan for ProxyExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        self.inner.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            unimplemented!("Should not be referenced during optimization")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(InputPartitionStream {
            schema: self.schema(),
            partition: self.inputs[partition].clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        self.inner.statistics()
    }
}

impl DisplayAs for ProxyExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProxyExecutionPlan")
    }
}
