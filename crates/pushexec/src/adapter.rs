use crate::errors::{PushExecError, Result};
use crate::partition::{BufferedPartition, BufferedPartitionStream};
use crate::pipeline::{Pipeline, PushPartitionId, Sink, Source};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{
    EquivalenceProperties, OrderingEquivalenceProperties, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use futures::StreamExt;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use tonic::async_trait;

/// A subplan represents part of a datafusion execution plan.
#[derive(Debug)]
pub struct SubPlan {
    /// The root of the subplan.
    pub root: Arc<dyn ExecutionPlan>,
    /// How deep the subplan goes.
    ///
    /// A depth of zero indicated the subplan is just the root.
    pub depth: usize,
}

/// Adapts a subplan of a datafusion execution plan.
pub struct ExecutionPlanAdapter {
    /// Input partitions indexed by child index and partition index.
    child_partitions: Vec<Vec<Arc<Mutex<BufferedPartition>>>>,
    /// Output streams.
    streams: Vec<Mutex<SendableRecordBatchStream>>,
}

impl ExecutionPlanAdapter {
    /// Create a new execution plan adapter from a subplan.
    ///
    /// Intermediate plans in the subplan must only have one child. The final
    /// child in the subplan may have any number of children.
    pub fn new(subplan: SubPlan, context: Arc<TaskContext>) -> Result<ExecutionPlanAdapter> {
        let mut plans = Vec::with_capacity(subplan.depth + 1);

        let mut curr = subplan.root;
        plans.push(curr.clone());

        // Get all intermediate plans.
        for _i in 0..subplan.depth {
            let mut children = curr.children();
            if children.len() != 1 {
                return Err(PushExecError::Static(
                    "Intermediate plan does not have exactly one child",
                ));
            }
            curr = children.pop().unwrap();
            plans.push(curr.clone());
        }

        let last = plans.pop().unwrap();

        // Replace last plan's children with shims if it has any.
        let children = last.children();
        let mut child_partitions = Vec::with_capacity(children.len());
        let last_with_shim = if children.is_empty() {
            last // Nothing to shim
        } else {
            let mut shims = Vec::with_capacity(children.len());

            for child in children {
                let partition_count = child.output_partitioning().partition_count();
                let partitions: Vec<_> = (0..partition_count)
                    .map(|_| Arc::new(Mutex::new(BufferedPartition::default())))
                    .collect();

                shims.push(Arc::new(ExecutionPlanShim {
                    inner: child,
                    partitions: partitions.clone(),
                }) as Arc<dyn ExecutionPlan>);
                child_partitions.push(partitions);
            }

            last.with_new_children(shims)?
        };

        // Rebuild subplan bottom up.
        let mut curr = last_with_shim;
        for plan in plans.into_iter().rev() {
            curr = plan.with_new_children(vec![curr])?;
        }

        // Collect output partition streams.
        let stream_count = curr.output_partitioning().partition_count();
        let mut streams = Vec::with_capacity(stream_count);
        for partition in 0..stream_count {
            let stream = curr.execute(partition, context.clone())?;
            streams.push(Mutex::new(stream));
        }

        Ok(ExecutionPlanAdapter {
            child_partitions,
            streams,
        })
    }
}

impl fmt::Debug for ExecutionPlanAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionPlanAdapter")
            .field("num_children", &self.child_partitions.len())
            .field("num_partitions", &self.child_partitions[0].len())
            .field("num_stream", &self.streams.len())
            .finish()
    }
}

impl Pipeline for ExecutionPlanAdapter {}

impl Sink for ExecutionPlanAdapter {
    fn push_partition(&self, input: RecordBatch, partition: PushPartitionId) -> Result<()> {
        let mut partition = self.child_partitions[partition.child][partition.idx].lock();
        partition.push_batch(input);
        Ok(())
    }

    fn finish(&self, partition: PushPartitionId) -> Result<()> {
        let mut part = self.child_partitions[partition.child][partition.idx].lock();
        part.finish();
        Ok(())
    }
}

impl Source for ExecutionPlanAdapter {
    fn output_partitions(&self) -> usize {
        self.streams.len()
    }

    fn poll_partition(
        &self,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let stream = self.streams[partition].lock().poll_next_unpin(cx);
        // Map df error to push exec
        stream.map(|o| o.map(|r| r.map_err(|e| PushExecError::DataFusion(e))))
    }
}

#[derive(Debug)]
struct ExecutionPlanShim {
    inner: Arc<dyn ExecutionPlan>,
    partitions: Vec<Arc<Mutex<BufferedPartition>>>,
}

#[async_trait]
impl ExecutionPlan for ExecutionPlanShim {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn unbounded_output(&self, _children: &[bool]) -> DataFusionResult<bool> {
        Ok(false)
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

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.inner.equivalence_properties()
    }

    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        self.inner.ordering_equivalence_properties()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "With children not implemented for execution plan shim".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let partition = self.partitions[partition].clone();
        Ok(Box::pin(BufferedPartitionStream::new(
            self.schema(),
            partition,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ExecutionPlanShim")
    }

    fn statistics(&self) -> Statistics {
        self.inner.statistics()
    }
}
