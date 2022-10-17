use crate::errors::Result;
use access::deltacache::DeltaCache;
use access::deltaexec::DeltaMergeExec;
use access::keys::{PartitionKey, TableId};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{
    display::DisplayableExecutionPlan, empty::EmptyExec, ExecutionPlan,
};
use dfutil::cast::cast_record_batch;
use std::any::Any;
use std::sync::Arc;
use tracing::trace;

/// An implementation of a table provider using our delta cache.
///
/// NOTE: This currently has a one-to-one mapping between table and partition.
/// There will be an additional layer between `DeltaMergeExec` and this to
/// combine access to multiple partitions for a table.
#[derive(Debug, Clone)]
pub struct DeltaTable {
    table_id: TableId,
    schema: SchemaRef,
    cache: Arc<DeltaCache>,
}

impl DeltaTable {
    pub fn new(table_id: TableId, schema: SchemaRef, cache: Arc<DeltaCache>) -> DeltaTable {
        DeltaTable {
            table_id,
            schema,
            cache,
        }
    }

    pub fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let key = PartitionKey {
            table_id: self.table_id,
            part_id: 0, // TODO: Need another layer of indirection.
        };
        let batch = cast_record_batch(batch, self.schema.clone())?;
        self.cache.insert_batch(&key, batch);
        Ok(())
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let key = PartitionKey {
            table_id: self.table_id,
            part_id: 0,
        };
        let empty = EmptyExec::new(false, self.schema.clone()); // TODO: Base partition scan.
        let plan = DeltaMergeExec::new(
            key,
            self.schema.clone(),
            self.cache.clone(),
            projection.clone(),
            Arc::new(empty),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        {
            let displayable = DisplayableExecutionPlan::new(&plan);
            trace!(plan = %displayable.indent(), "table scan execution plan");
        }

        Ok(Arc::new(plan))
    }
}
