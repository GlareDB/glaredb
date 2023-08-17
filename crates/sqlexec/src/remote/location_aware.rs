use crate::planner::physical_plan::client_recv::ClientExchangeRecvExec;
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
use uuid::Uuid;

/// Who should be scanning the table.
#[derive(Debug, Clone, Copy)]
pub enum ScanLocation {
    /// Client should scan the table (e.g. local CSV files or data frames).
    ///
    /// This will require a data exchange.
    Client {
        /// ID to use when constructing the recv exec.
        ///
        /// When the server begins executing, it will attempt to pull from the
        /// recv exec. The client will be responsible for sending record batches
        /// to the server using this ID.
        broadcast_id: Uuid,
    },
    /// Server should scan the table.
    Server,
}

/// A table provider that is aware of where it should be scanned from.
///
/// Currently this is assuming that the server is executing this plan.
pub struct LocationAwareTableProvider {
    pub inner: Arc<dyn TableProvider>,
    pub location: ScanLocation,
}

impl LocationAwareTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>, location: ScanLocation) -> Self {
        Self { inner, location }
    }
}

#[async_trait]
impl TableProvider for LocationAwareTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.inner.get_logical_plan()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.location {
            ScanLocation::Client { broadcast_id } => {
                // Is it really this easy?
                let plan = ClientExchangeRecvExec::new(broadcast_id, self.schema());
                Ok(Arc::new(plan))
            }
            ScanLocation::Server => {
                // Currently no changes needed.
                let plan = self.inner.scan(state, projection, filters, limit).await?;
                Ok(plan)
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.insert_into(state, input).await?;
        Ok(plan)
    }
}
