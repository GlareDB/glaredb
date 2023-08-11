use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan as DfLogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;
use uuid::Uuid;

use crate::planner::logical_plan::CreateTable;

use super::client::AuthenticatedExecutionServiceClient;
use super::exec::RemoteLogicalExec;

/// A planner that executes everything on a remote service.
#[derive(Debug, Clone)]
pub struct RemoteQueryPlanner {
    session_id: Uuid,
    /// Client to remote services.
    client: AuthenticatedExecutionServiceClient,
}

impl RemoteQueryPlanner {
    pub fn new(
        session_id: Uuid,
        client: AuthenticatedExecutionServiceClient,
    ) -> RemoteQueryPlanner {
        RemoteQueryPlanner { session_id, client }
    }
}

#[async_trait]
impl QueryPlanner for RemoteQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        println!("RemoteQueryPlanner::create_physical_plan");
        Ok(Arc::new(RemoteLogicalExec::new(
            self.session_id,
            self.client.clone(),
            logical_plan.clone(),
        )))
    }
}
