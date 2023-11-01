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
use protogen::metastore::types::catalog::{AllowedOperations, RuntimePreference};
use std::{any::Any, sync::Arc};

use super::runtime_group::RuntimeGroupExec;

/// A table provider that has an associated runtime preference.
///
/// During a scan, this will produce a `RuntimeGroupExec` which is just a
/// passthrough to the underlying execution plan.
#[derive(Clone)]
pub struct RuntimeAwareTableProvider {
    pub preference: RuntimePreference,
    pub provider: Arc<dyn TableProvider>,
    pub allowed_operations: AllowedOperations,
}

impl RuntimeAwareTableProvider {
    pub fn new(preference: RuntimePreference, provider: Arc<dyn TableProvider>) -> Self {
        Self {
            preference,
            provider,
            allowed_operations: AllowedOperations::new(),
        }
    }
}

#[async_trait]
impl TableProvider for RuntimeAwareTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.provider.schema()
    }

    fn table_type(&self) -> TableType {
        self.provider.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.provider.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.provider.get_logical_plan()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .provider
            .scan(state, projection, filters, limit)
            .await?;
        Ok(Arc::new(RuntimeGroupExec {
            preference: self.preference,
            child: plan,
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.provider.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.provider.statistics()
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.provider.insert_into(state, input, overwrite).await?;
        Ok(plan)
    }
}
