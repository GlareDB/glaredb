use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    datasource::TableProvider,
    error::{DataFusionError, Result as DfResult},
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use uuid::Uuid;

use crate::{
    errors::Result,
    planner::physical_plan::remote_scan::{ProviderReference, RemoteScanExec},
};

/// A stub table provider for getting the schema of a remote table.
///
/// When the local session needs information for a table that exists in an
/// external system (e.g. Postgres), it will call out to the remote session. The
/// remote session loads the actual table, but than sends back this stubbed
/// provider so that the local session can use it for query planning.
#[derive(Debug)]
pub struct StubRemoteTableProvider {
    /// ID for this table provider.
    provider_id: Uuid,
    /// Schema for the table provider.
    schema: Arc<Schema>,
}

impl StubRemoteTableProvider {
    pub fn new(provider_id: Uuid, schema: SchemaRef) -> Self {
        Self {
            provider_id,
            schema,
        }
    }

    /// Returns the provider ID.
    pub fn id(&self) -> Uuid {
        self.provider_id
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(self.provider_id.as_bytes());
        Ok(())
    }
}

#[async_trait]
impl TableProvider for StubRemoteTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let provider = ProviderReference::RemoteReference(self.provider_id);
        let projected_schema = match projection {
            Some(proj) => Arc::new(self.schema.project(proj)?),
            None => self.schema.clone(),
        };

        let exec = RemoteScanExec::new(
            provider,
            projected_schema,
            projection.cloned(),
            filters.to_vec(),
            limit,
        );

        Ok(Arc::new(exec))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "insert_into called on a stub provider".to_string(),
        ))
    }
}
