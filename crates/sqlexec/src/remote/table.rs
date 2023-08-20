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

use crate::errors::Result;

/// A stub table provider for getting the schema of a remote table.
///
/// When the local session needs information for a table that exists in an
/// external system (e.g. Postgres), it will call out to the remote session. The
/// remote session loads the actual table, but than sends back this stubbed
/// provider so that the local session can use it for query planning.
///
/// Attempting to scan or insert will error.
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "scan called on a stub provider".to_string(),
        ))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "insert_into called on a stub provider".to_string(),
        ))
    }
}
