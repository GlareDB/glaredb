use crate::errors::{PushExecError, Result};
use crate::operator::{Pipeline, Sink, Source};
use crate::partition::{BufferedPartition, BufferedPartitionStream};
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
use parking_lot::Mutex;
use std::any::Any;
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

/// Adapts a subpan whose child plans never have more than one child.
///
/// This subplan will be reprented as a single `Pipeline`.
pub struct BranchlessAdapter {
    /// Input partitions.
    partitions: Vec<Arc<Mutex<BufferedPartition>>>,
    /// Output streams.
    streams: Vec<SendableRecordBatchStream>,
}

impl BranchlessAdapter {
    /// Create a new execution plan adapter from a subplan.
    pub fn new(subplan: SubPlan, context: Arc<TaskContext>) -> Result<BranchlessAdapter> {
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

        // Last child may have 0 or 1 children.
        let last = plans.pop().unwrap();
        if last.children().len() > 1 {
            return Err(PushExecError::Static(
                "Last child plan does not have zero or one children",
            ));
        }

        // Last child determines partitioning.
        let partition_count = last.output_partitioning().partition_count();
        let partitions: Vec<_> = (0..partition_count)
            .map(|_| Arc::new(Mutex::new(BufferedPartition::default())))
            .collect();

        // Replace last plan's child if it has one with a shim.
        let last_with_shim = match last.children().len() {
            0 => last.clone(), // Nothing to shim.
            1 => {
                let inner = last.clone();
                last.with_new_children(vec![Arc::new(ExecutionPlanShim {
                    inner,
                    partitions: partitions.clone(),
                })])?
            }
            _ => unreachable!(), // All plans were checked to ensure at most one child.
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
            streams.push(stream);
        }

        Ok(BranchlessAdapter {
            partitions,
            streams,
        })
    }
}

impl Pipeline for BranchlessAdapter {}

impl Sink for BranchlessAdapter {
    fn push_partition(&self, input: RecordBatch, partition: usize) -> Result<()> {
        let mut partition = self.partitions[partition].lock();
        partition.push_batch(input);
        Ok(())
    }

    fn finish(&self, partition: usize) -> Result<()> {
        let mut part = self.partitions[partition].lock();
        part.finish();
        Ok(())
    }

    fn input_partitions(&self) -> usize {
        self.partitions.len()
    }
}

impl Source for BranchlessAdapter {
    fn output_partitions(&self) -> usize {
        self.streams.len()
    }

    fn poll_partition(
        &self,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut partition = self.partitions[partition].lock();
        match partition.pop_batch() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if partition.is_finished() => Poll::Ready(None),
            None => {
                partition.register_waker(cx.waker().clone());
                Poll::Pending
            }
        }
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
