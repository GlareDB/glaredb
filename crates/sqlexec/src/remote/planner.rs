use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{Extension, LogicalPlan as DfLogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;

use std::sync::Arc;

use crate::errors::internal;
use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::CreateCredentials;
use crate::planner::physical_plan::create_credentials_exec::CreateCredentialsExec;
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
pub struct DDLExtensionPlanner;
#[async_trait]
impl ExtensionPlanner for DDLExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&DfLogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let extension_type = node.name().parse::<ExtensionType>().unwrap();

        match extension_type {
            ExtensionType::AlterDatabaseRename => todo!(),
            ExtensionType::AlterTableRename => todo!(),
            ExtensionType::AlterTunnelRotateKeys => todo!(),
            ExtensionType::CreateCredentials => {
                let create_credentials_lp = match node.as_any().downcast_ref::<CreateCredentials>()
                {
                    Some(s) => Ok(s.clone()),
                    None => Err(internal!("CreateCredentials::try_decode_extension failed",)),
                }
                .unwrap();

                let exec = CreateCredentialsExec {
                    name: create_credentials_lp.name,
                    options: create_credentials_lp.options,
                    comment: create_credentials_lp.comment,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::CreateExternalDatabase => todo!(),
            ExtensionType::CreateExternalTable => todo!(),
            ExtensionType::CreateSchema => todo!(),
            ExtensionType::CreateTable => todo!(),
            ExtensionType::CreateTempTable => todo!(),
            ExtensionType::CreateTunnel => todo!(),
            ExtensionType::CreateView => todo!(),
            ExtensionType::DropTables => todo!(),
            ExtensionType::DropCredentials => todo!(),
            ExtensionType::DropDatabase => todo!(),
            ExtensionType::DropSchemas => todo!(),
            ExtensionType::DropTunnel => todo!(),
            ExtensionType::DropViews => todo!(),
            ExtensionType::SetVariable => todo!(),
            ExtensionType::CopyTo => todo!(),
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
        // TODO: This can be refactored a bit to better handle explains.

        // Rewrite logical plan to ensure tables that have a
        // "local" hint have their providers replaced with ones
        // that will produce client recv and send execs.
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
        let logical_plan = logical_plan.clone();
        let logical_plan = logical_plan.rewrite(&mut rewriter)?;

        // Create the physical plans. This will call `scan` on
        // the custom table providers meaning we'll have the
        // correct exec refs.
        let physical =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(DDLExtensionPlanner)])
                .create_physical_plan(&logical_plan, session_state)
                .await?;

        // Temporary coalesce exec until our custom plans support partition.
        let physical = Arc::new(CoalescePartitionsExec::new(physical));

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
