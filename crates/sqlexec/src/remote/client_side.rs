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
use uuid::Uuid;

use super::client::RemoteSessionClient;

#[derive(Clone, Default)]
pub struct ClientExecRef {
    /// The exec for client send.
    ///
    /// Note that this is only set after a call to scan.
    pub exec: Arc<Mutex<Option<ClientExchangeSendExec>>>,
}

impl ClientExecRef {
    pub fn take_exec(&self) -> Option<ClientExchangeSendExec> {
        let mut exec = self.exec.lock();
        exec.take()
    }
}

/// A table provider that requires scanning to happen on the client side.
///
/// What this means is that during a scan, the execution plan will be a recv
/// exec which should get sent over to the remote node. The client will make a
/// send exec using this provider's broadcast id.
///
/// This should only be used when running glaredb in a distributed mode
/// (client+server).
pub struct ClientSideTableProvider {
    /// The inner table for the data.
    ///
    /// The client will be reading this table to send on the stream.
    inner: Arc<dyn TableProvider>,
    /// Client to the remote node.
    client: RemoteSessionClient,
    /// Exec reference to populate after scan.
    exec_ref: ClientExecRef,
}

impl ClientSideTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>, client: RemoteSessionClient) -> Self {
        Self {
            inner,
            client,
            exec_ref: ClientExecRef::default(),
        }
    }

    pub fn get_exec_ref(&self) -> ClientExecRef {
        self.exec_ref.clone()
    }
}

#[async_trait]
impl TableProvider for ClientSideTableProvider {
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
        let broadcast_id = Uuid::new_v4();
        let recv = ClientExchangeRecvExec::new(broadcast_id, self.schema());

        let input = self.inner.scan(state, projection, filters, limit).await?;
        let send = ClientExchangeSendExec::new(broadcast_id, self.client.clone(), input);

        let mut exec_ref = self.exec_ref.exec.lock();
        *exec_ref = Some(send); // TODO: Error if already Some?

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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.insert_into(state, input).await?;
        Ok(plan)
    }
}
