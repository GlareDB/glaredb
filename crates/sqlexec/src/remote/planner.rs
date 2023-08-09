use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uuid::Uuid;

use super::client::AuthenticatedExecutionServiceClient;
use super::exec::RemoteLogicalExec;

/// A planner that executes everything on a remote service.
#[derive(Debug, Clone)]
pub struct RemotePlanner {
    session_id: Uuid,
    /// Client to remote services.
    client: AuthenticatedExecutionServiceClient,
}

impl RemotePlanner {
    pub fn new(session_id: Uuid, client: AuthenticatedExecutionServiceClient) -> RemotePlanner {
        RemotePlanner { session_id, client }
    }
}

#[async_trait]
impl QueryPlanner for RemotePlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(RemoteLogicalExec::new(
            self.session_id,
            self.client.clone(),
            logical_plan.clone(),
        )))
    }
}
