use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableType},
    physical_plan::{ExecutionPlan, Statistics},
    prelude::Expr,
};
use std::{any::Any, sync::Arc};

/// See if the provider is wrapped in a local table hint.
///
/// This can be used to swap out the provider with one that's able to do hybrid
/// execution between local and remote nodes.
pub fn is_local_table_hint(provider: &Arc<dyn TableProvider>) -> bool {
    provider.as_ref().as_any().is::<LocalTableHint>()
}

/// A table provider that provides a hint for if the table is "local" to the
/// session doing the planning.
///
/// This can wrap things like data frames and local file readers such that
/// they're planned to be scanned on the client.
#[derive(Clone)]
pub struct LocalTableHint(pub Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for LocalTableHint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.0.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.0.get_logical_plan()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.0.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.0.statistics()
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.0.insert_into(state, input, overwrite).await?;
        Ok(plan)
    }
}
