use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
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
impl QueryPlanner for RemoteLogicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let physical_planner = RemotePhysicalPlanner::from(self);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Debug, Clone)]
pub struct RemotePhysicalPlanner {
    session_id: Uuid,
    /// Client to remote services.
    client: AuthenticatedExecutionServiceClient,
}

impl From<&RemoteLogicalPlanner> for RemotePhysicalPlanner {
    fn from(planner: &RemoteLogicalPlanner) -> Self {
        Self {
            session_id: planner.session_id,
            client: planner.client.clone(),
        }
    }
}

#[async_trait]
impl PhysicalPlanner for RemotePhysicalPlanner {
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
    fn create_physical_expr(
        &self,
        _expr: &Expr,
        _input_dfschema: &DFSchema,
        _input_schema: &Schema,
        _session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        todo!("create_physical_expr")
    }
}
