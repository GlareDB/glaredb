use crate::deltacache::DeltaCache;
use crate::errors::Result;
use catalog_types::keys::PartitionKey;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, memory::MemoryStream, ExecutionPlan,
    Partitioning, SendableRecordBatchStream, Statistics,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Create a stream of record batches from insert deltas for a single partition.
// TODO: Provide optional limit as well.
#[derive(Debug)]
pub struct DeltaInsertsExec {
    partition: PartitionKey,
    output_schema: SchemaRef,
    deltas: Arc<DeltaCache>,
    projection: Option<Vec<usize>>,
}

impl DeltaInsertsExec {
    pub fn new(
        partition: PartitionKey,
        input_schema: SchemaRef,
        cache: Arc<DeltaCache>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let output_schema = match &projection {
            Some(projection) => Arc::new(input_schema.project(projection)?),
            None => input_schema,
        };
        Ok(DeltaInsertsExec {
            partition,
            output_schema,
            deltas: cache,
            projection,
        })
    }
}

impl ExecutionPlan for DeltaInsertsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for DeltaInsertsExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let batches = self.deltas.partition_inserts(&self.partition);
        let stream =
            MemoryStream::try_new(batches, self.output_schema.clone(), self.projection.clone())?;
        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DeltaInsertsExec: part={}, projection={:?}",
            self.partition, self.projection
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
