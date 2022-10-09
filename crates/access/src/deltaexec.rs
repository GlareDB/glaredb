use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, Distribution, ExecutionPlan,
    Partitioning, SendableRecordBatchStream, Statistics,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// A physical plan node for returning delta record batches for a single
/// partition.
#[derive(Debug)]
pub struct DeltaExec {}

#[async_trait]
impl ExecutionPlan for DeltaExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        unimplemented!()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn maintains_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        unimplemented!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}
