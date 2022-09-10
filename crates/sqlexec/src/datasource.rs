use crate::errors::{internal, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemoryTableProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for MemoryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        unimplemented!()
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[self.batches.clone()],
            self.schema.clone(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> DfResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
