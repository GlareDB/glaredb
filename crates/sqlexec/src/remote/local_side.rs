use crate::planner::physical_plan::{
    client_recv::ClientExchangeRecvExec, client_send::ClientExchangeSendExec,
};
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
use parking_lot::Mutex;
use std::{any::Any, sync::Arc};
use tracing::debug;
use uuid::Uuid;

use super::client::RemoteSessionClient;

#[derive(Clone, Default)]
pub struct ClientSendExecsRef {
    /// The execs for client send.
    ///
    /// Each call to `scan` will push a new send exec. Each send exec will send
    /// on its own stream to the remote node.
    execs: Arc<Mutex<Vec<ClientExchangeSendExec>>>,
}

impl ClientSendExecsRef {
    pub fn take_execs(&self) -> Vec<ClientExchangeSendExec> {
        let mut execs = self.execs.lock();
        std::mem::take(execs.as_mut())
    }
}

/// A table provider that requires scanning to happen locally and not on the
/// server.
///
/// What this means is that during a scan, the execution plan will be a recv
/// exec which should get sent over to the remote node. The client will make a
/// send exec using this provider's broadcast id.
///
/// This should only be used when running glaredb in a distributed mode
/// (client+server).
pub struct LocalSideTableProvider {
    /// The inner table for the data.
    ///
    /// The client will be reading this table to send on the stream.
    inner: Arc<dyn TableProvider>,
    /// Client to the remote node.
    client: RemoteSessionClient,
    /// Exec reference to populate after scan.
    exec_ref: ClientSendExecsRef,
}

impl LocalSideTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>, client: RemoteSessionClient) -> Self {
        Self {
            inner,
            client,
            exec_ref: ClientSendExecsRef::default(),
        }
    }

    pub fn get_exec_ref(&self) -> ClientSendExecsRef {
        self.exec_ref.clone()
    }
}

#[async_trait]
impl TableProvider for LocalSideTableProvider {
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
        let input = self.inner.scan(state, projection, filters, limit).await?;

        let broadcast_id = Uuid::new_v4();
        debug!(%broadcast_id, "creating send and recv execs");

        // Create the receive exec. This will be executed on the remote node.
        let recv = ClientExchangeRecvExec {
            broadcast_id,
            schema: input.schema(),
        };

        // And create the associated send exec. This will be executed locally,
        // and pushes batches over the broadcast endpoint.
        let send = ClientExchangeSendExec {
            broadcast_id,
            client: self.client.clone(),
            input,
        };

        let mut execs = self.exec_ref.execs.lock();
        execs.push(send);

        Ok(Arc::new(recv))
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
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.insert_into(state, input, overwrite).await?;
        Ok(plan)
    }
}
