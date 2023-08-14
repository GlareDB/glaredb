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

use crate::errors::{ExecError, Result};

use super::{client::RemoteSessionClient, exec::RemoteExecutionPlan};

#[derive(Debug)]
pub struct RemoteTableProvider {
    /// ID for this table provider.
    provider_id: Uuid,
    /// Schema for the table provider.
    schema: Arc<Schema>,
    /// Client for remote services.
    _client: RemoteSessionClient,
}

impl RemoteTableProvider {
    pub fn new(client: RemoteSessionClient, provider_id: Uuid, schema: SchemaRef) -> Self {
        Self {
            provider_id,
            schema,
            _client: client,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(self.provider_id.as_bytes());
        Ok(())
    }
}

#[async_trait]
impl TableProvider for RemoteTableProvider {
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
        Err(DataFusionError::External(Box::new(
            ExecError::UnsupportedFeature("Table scan over RPC"),
        )))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let _input = input
            .as_any()
            .downcast_ref::<RemoteExecutionPlan>()
            .ok_or_else(|| {
                DataFusionError::External(Box::new(ExecError::Internal(
                    "`ExecutionPlan` not a `RemoteExecutionPlan` for remote `insert_into`"
                        .to_string(),
                )))
            })?;

        Err(DataFusionError::External(Box::new(
            ExecError::UnsupportedFeature("INSERT INTO on RPC"),
        )))
    }
}
