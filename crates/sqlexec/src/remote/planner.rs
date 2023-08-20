use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::display::ToStringifiedPlan;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan as DfLogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{displayable, ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;

use std::sync::Arc;

use crate::planner::physical_plan::remote_exec::RemoteExecutionExec;
use crate::planner::physical_plan::send_recv::SendRecvJoinExec;

use super::client::RemoteSessionClient;
use super::rewriter::LocalSideTableRewriter;

/// Generates physical plans for executing on a remote service.
#[derive(Debug, Clone)]
pub struct RemoteQueryPlanner {
    /// Client to remote services.
    remote_client: RemoteSessionClient,
}

impl RemoteQueryPlanner {
    pub fn new(client: RemoteSessionClient) -> RemoteQueryPlanner {
        RemoteQueryPlanner {
            remote_client: client,
        }
    }
}

#[async_trait]
impl QueryPlanner for RemoteQueryPlanner {
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
    remote_client: RemoteSessionClient,
}

impl From<&RemoteQueryPlanner> for RemotePhysicalPlanner {
    fn from(planner: &RemoteQueryPlanner) -> Self {
        Self {
            remote_client: planner.remote_client.clone(),
        }
    }
}

#[async_trait]
impl PhysicalPlanner for RemotePhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Rewrite logical plan to ensure tables that have a
        // "local" hint have their providers replaced with ones
        // that will produce client recv and send execs.
        println!("before: {:?}", logical_plan);
        let mut rewriter = LocalSideTableRewriter::new(self.remote_client.clone());
        // TODO: Figure out where this should actually be called to remove need
        // to clone.
        //
        // Datafusion steps:
        // 1. Analyze logical
        // 2. Optimize logical
        // 3. Plan physical
        // 4. Optimize physical
        //
        // Ideally this rewrite happens between 2 and 3. We could also add an
        // optimzer rule to do this, but that kinda goes against what
        // optimzation is for. We would also need to figure out how to pass
        // around the exec refs, so probably not worth it.
        let what_plan = logical_plan.clone();
        let what_plan = what_plan.rewrite(&mut rewriter)?;

        println!("after: {:?}", logical_plan);
        println!("execs len: {}", rewriter.exec_refs.len());

        // Create the physical plans. This will call `scan` on
        // the custom table providers meaning we'll have the
        // correct exec refs.
        let physical = DefaultPhysicalPlanner::default()
            .create_physical_plan(&what_plan, session_state)
            .await?;

        // Wrap in exec that will send the plan to the remote machine.
        let physical = Arc::new(RemoteExecutionExec::new(
            self.remote_client.clone(),
            physical,
        ));

        // Create a wrapper physical plan which drives both the
        // result stream, and the send execs
        //
        // At this point, the send execs should have been
        // populated.
        let physical = Arc::new(SendRecvJoinExec::new(physical, rewriter.exec_refs));

        let displayable_plan = displayable(physical.as_ref());
        println!("{}", displayable_plan.indent(true));

        Ok(physical)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        DefaultPhysicalPlanner::default().create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )
    }
}
