use crate::errors::{Result, RpcsrvError};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::protobuf::LogicalPlanNode;
use protogen::gen::rpcsrv::service::execute_request::Plan;
use protogen::gen::rpcsrv::service::ExecuteRequest;
use protogen::metastore::types::catalog::CatalogState;
use sqlexec::engine::TrackedSession;
use sqlexec::extension_codec::GlareDBExtensionCodec;
use std::sync::Arc;
use tokio::sync::Mutex;

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

    pub async fn execute_serialized_plan(
        &self,
        req: ExecuteRequest,
    ) -> Result<SendableRecordBatchStream> {
        match req.plan {
            Some(Plan::LogicalPlan(buf)) => {
                // TODO: Use a context that actually matters.
                let fake_ctx = SessionContext::new();
                let plan = LogicalPlanNode::try_decode(&buf)?
                    .try_into_logical_plan(&fake_ctx, &GlareDBExtensionCodec)?;
                println!("SERVER---\nplan: {:?}", plan);
                let mut session = self.session.lock().await;
                let physical = session.create_physical_plan(plan).await?;
                println!("SERVER---\nphysical: {:?}", physical);
                let stream = session.execute_physical(physical)?;

                Ok(stream)
            }
            Some(Plan::PhysicalPlan(_)) => Err(RpcsrvError::PhysicalPlansNotSupported),
            None => Err(RpcsrvError::Internal("missing plan on request".to_string())),
        }
    }
}
