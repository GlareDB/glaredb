use crate::pipeline::Pipeline;
use crate::errors::{ PushExecError, Result };
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub struct ExecutionPipeline {
    proxied: Arc<dyn ExecutionPlan>,
    inputs: Vec<Vec<Arc<Mutex<InputPartition>>>>,
    outputs: Vec<Mutex<SendableRecordBatchStream>>,
}

impl std::fmt::Debug for ExecutionPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ExecutionNode").field(&self.proxied).finish()
    }
}

impl ExecutionPipeline {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
        depth: usize,
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

        // Construct the output streams
        let output_count = proxied.output_partitioning().partition_count();

        let mut outputs = Vec::with_capacity(output_count);
        for i in 0..output_count {
            let stream = proxied.execute(i, task_context.clone())?;
            outputs.push(Mutex::new(stream))
        }

        Ok(Self {
            proxied,
            inputs,
            outputs,
        })
    }
}

impl Pipeline for ExecutionPipeline {
    /// Push a `RecordBatch` to the given input partition
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        let mut partition = self.inputs[child][partition].lock();
        assert!(!partition.is_closed);

        partition.buffer.push_back(input);
        for waker in partition.wait_list.drain(..) {
            waker.wake()
        }
        Ok(())
    }

    fn close(&self, child: usize, partition: usize) {
        let mut partition = self.inputs[child][partition].lock();
        assert!(!partition.is_closed);

        partition.is_closed = true;
        for waker in partition.wait_list.drain(..) {
            waker.wake()
        }
    }

    fn output_partitions(&self) -> usize {
        self.outputs.len()
    }

    /// Poll an output partition, attempting to get its output
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        self.outputs[partition]
            .lock()
            .poll_next_unpin(cx)
            .map(|opt| opt.map(|r| r.map_err(Into::into)))
    }
}

#[derive(Debug, Default)]
struct InputPartition {
    buffer: VecDeque<RecordBatch>,
    wait_list: Vec<Waker>,
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
                partition.wait_list.push(cx.waker().clone());
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

    fn benefits_from_input_partitioning(&self) -> bool {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(InputPartitionStream {
            schema: self.schema(),
            partition: self.inputs[partition].clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ProxyExecutionPlan")
    }

    fn statistics(&self) -> Statistics {
        self.inner.statistics()
    }
}
