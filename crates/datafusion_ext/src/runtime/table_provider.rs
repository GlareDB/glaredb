use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::Expr;
use protogen::metastore::types::catalog::RuntimePreference;

use super::runtime_group::RuntimeGroupExec;

/// A table provider that has an associated runtime preference.
///
/// During a scan, this will produce a `RuntimeGroupExec` which is just a
/// passthrough to the underlying execution plan.
#[derive(Clone)]
pub struct RuntimeAwareTableProvider {
    pub preference: RuntimePreference,
    pub provider: Arc<dyn TableProvider>,
}

impl RuntimeAwareTableProvider {
    pub fn new(preference: RuntimePreference, provider: Arc<dyn TableProvider>) -> Self {
        Self {
            preference,
            provider,
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
