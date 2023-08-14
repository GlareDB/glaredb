use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::ExecutionPlan;

use std::sync::Arc;

use super::client::RemoteSessionClient;

/// A planner that executes everything on a remote service.
#[derive(Debug, Clone)]
pub struct RemotePlanner {
    /// Client to remote services.
    client: RemoteSessionClient,
}

impl RemotePlanner {
    pub fn new(client: RemoteSessionClient) -> RemotePlanner {
        RemotePlanner { client }
    }
}

#[async_trait]
impl QueryPlanner for RemotePlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut client = self.client.clone();
        let physical_plan = client
            .create_physical_plan(logical_plan)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(physical_plan))
    }
}
