use crate::errors::{internal, Result};
use arrowstore::proto::arrow_store_service_server::ArrowStoreService;
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

/// Implements the table provider for datafusion using a local arrow store
/// service.
pub struct LocalArrowStoreTable<S> {
    schema: SchemaRef,
    /// Identifier for the table.
    table: String,
    /// The underlying arrow store. Note that this will be shared across
    /// different table providers.
    local: S,
}

impl<S: ArrowStoreService> LocalArrowStoreTable<S> {
    pub fn new(store: S, schema: SchemaRef, table: impl Into<String>) -> Self {
        LocalArrowStoreTable {
            schema,
            table: table.into(),
            local: store,
        }
    }
}

#[async_trait]
impl<S: ArrowStoreService> TableProvider for LocalArrowStoreTable<S> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
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
        unimplemented!()
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> DfResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
