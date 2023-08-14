use crate::errors::{Result, RpcsrvError};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::OwnedTableReference;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::protobuf::LogicalPlanNode;
use protogen::metastore::types::catalog::CatalogState;
use protogen::rpcsrv::types::service::{PhysicalPlanResponse, TableProviderResponse};
use sqlexec::engine::TrackedSession;
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

    pub async fn create_physical_plan(
        &self,
        logical_plan: impl AsRef<[u8]>,
    ) -> Result<PhysicalPlanResponse> {
        let mut session = self.session.lock().await;

        // TODO: Use a context that actually matters.
        let fake_ctx = SessionContext::new();
        let plan = LogicalPlanNode::try_decode(logical_plan.as_ref())?
            .try_into_logical_plan(&fake_ctx, &session.extension_codec())?;

        let physical = session.create_physical_plan(plan).await?;
        let schema = physical.schema();
        let exec_id = session.add_physical_plan(physical);

        Ok(PhysicalPlanResponse {
            id: exec_id,
            schema: Schema::clone(&schema),
        })
    }

    pub async fn dispatch_access(
        &self,
        table_ref: OwnedTableReference,
    ) -> Result<TableProviderResponse> {
        let mut session = self.session.lock().await;
        let provider = session.dispatch_access(table_ref).await?;
        let schema = provider.schema();
        let provider_id = session.add_table_provider(provider);

        Ok(TableProviderResponse {
            id: provider_id,
            schema: Schema::clone(&schema),
        })
    }

    pub async fn physical_plan_execute(&self, exec_id: Uuid) -> Result<SendableRecordBatchStream> {
        let session = self.session.lock().await;
        let plan = session
            .get_physical_plan(&exec_id)
            .ok_or_else(|| RpcsrvError::MissingPhysicalPlan(exec_id))?;
        // TODO: Use a context that actually matters.
        let stream = session.execute_physical(plan)?;
        Ok(stream)
    }
}
