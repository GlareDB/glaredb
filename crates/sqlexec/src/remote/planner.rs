use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{Explain, Expr, LogicalPlan as DfLogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use protogen::gen::rpcsrv::service::execution_service_client::ExecutionServiceClient;
use std::sync::Arc;
use tonic::transport::Channel;
use uuid::Uuid;

use super::exec::RemoteLogicalExec;

#[derive(Debug, Clone)]
pub struct TestPlannerPleaseIgnore {
    session_id: Uuid,
    /// Client to remote services.
    client: ExecutionServiceClient<Channel>,
}

impl TestPlannerPleaseIgnore {
    pub fn new(
        session_id: Uuid,
        client: ExecutionServiceClient<Channel>,
    ) -> TestPlannerPleaseIgnore {
        TestPlannerPleaseIgnore { session_id, client }
    }
}

#[async_trait]
impl QueryPlanner for TestPlannerPleaseIgnore {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(RemoteLogicalExec::new(
            self.session_id,
            self.client.clone(),
            logical_plan.clone(),
        )))
    }
}
