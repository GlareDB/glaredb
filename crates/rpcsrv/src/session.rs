use crate::errors::Result;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::OwnedTableReference;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::Expr;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::protobuf::LogicalPlanNode;
use protogen::metastore::types::catalog::CatalogState;
use sqlexec::engine::TrackedSession;
use sqlexec::remote::exchange_stream::ClientExchangeRecvStream;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// A wrapper around a normal session for sql execution.
#[derive(Clone)]
pub struct RemoteSession {
    /// Inner sql session.
    ///
    /// Wrapped in an Arc and Mutex since the lifetime of the session is not
    /// tied to a single connection, and so needs to be tracked in a shared map.
    session: Arc<Mutex<TrackedSession>>,
}

impl RemoteSession {
    pub fn new(session: TrackedSession) -> Self {
        RemoteSession {
            session: Arc::new(Mutex::new(session)),
        }
    }

    /// Get the catalog state suitable for sending back to the requesting
    /// session.
    pub async fn get_catalog_state(&self) -> CatalogState {
        let session = self.session.lock().await;
        session.get_session_catalog().get_state().as_ref().clone()
    }

    pub async fn dispatch_access(&self, table_ref: OwnedTableReference) -> Result<(Uuid, Schema)> {
        let mut session = self.session.lock().await;
        let provider = session.dispatch_access(table_ref).await?;
        let schema = provider.schema();
        let provider_id = session.add_table_provider(provider)?;
        Ok((provider_id, Schema::clone(&schema)))
    }

    pub async fn table_provider_scan(
        &self,
        provider_id: Uuid,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(Uuid, Schema)> {
        let mut session = self.session.lock().await;
        let provider = session.get_table_provider(&provider_id)?;
        let physical = provider
            .scan(&session.df_ctx().state(), projection, filters, limit)
            .await?;
        let schema = physical.schema();
        let exec_id = session.add_physical_plan(physical)?;
        Ok((exec_id, Schema::clone(&schema)))
    }

    pub async fn table_provider_insert_into(
        &self,
        provider_id: Uuid,
        input_exec_id: Uuid,
    ) -> Result<(Uuid, Schema)> {
        let mut session = self.session.lock().await;
        let provider = session.get_table_provider(&provider_id)?;
        let input = session.get_physical_plan(&input_exec_id)?;
        let physical = provider
            .insert_into(&session.df_ctx().state(), input)
            .await?;
        let schema = physical.schema();
        let exec_id = session.add_physical_plan(physical)?;
        Ok((exec_id, Schema::clone(&schema)))
    }

    pub async fn physical_plan_execute(&self, exec_id: Uuid) -> Result<SendableRecordBatchStream> {
        let session = self.session.lock().await;
        let plan = session.get_physical_plan(&exec_id)?;
        let stream = session.execute_physical(plan)?;
        Ok(stream)
    }

    pub async fn register_broadcast_stream(&self, stream: ClientExchangeRecvStream) -> Result<()> {
        let session = self.session.lock().await;
        let streams = session.staged_streams()?;
        streams.put_stream(stream.broadcast_id(), stream);
        Ok(())
    }
}
