use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
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
    planner::physical_plan::{
        client_recv::ClientExchangeRecvExec,
        client_send::ClientExchangeSendExec,
        remote_exec::RemoteExecutionExec,
        remote_insert::{RemoteInsertExec, INSERT_PLAN_SCHEMA},
        remote_scan::{ProviderReference, RemoteScanExec},
        send_recv::SendRecvJoinExec,
    },
};

use super::{client::RemoteSessionClient, local_side::ClientSendExecsRef};

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
    /// Client for connecting to remote.
    client: RemoteSessionClient,
}

impl StubRemoteTableProvider {
    pub fn new(provider_id: Uuid, schema: SchemaRef, client: RemoteSessionClient) -> Self {
        Self {
            provider_id,
            schema,
            client,
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let provider = ProviderReference::RemoteReference(self.provider_id);
        let projected_schema = match projection {
            Some(proj) => Arc::new(self.schema.project(proj)?),
            None => self.schema.clone(),
        };

        let exec = RemoteScanExec {
            provider,
            projected_schema,
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        };

        Ok(Arc::new(exec))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let broadcast_id = Uuid::new_v4();
        let send = ClientExchangeSendExec {
            broadcast_id,
            client: self.client.clone(),
            input,
        };

        let recv = RemoteExecutionExec::new(
            self.client.clone(),
            Arc::new(RemoteInsertExec::from_broadcast(
                broadcast_id,
                ProviderReference::RemoteReference(self.provider_id),
            )),
        );

        let exec = SendRecvJoinExec::from_send_execs(Arc::new(recv), vec![send]);
        Ok(Arc::new(exec))
    }
}
