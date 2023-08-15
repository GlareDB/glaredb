use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use protogen::metastore::types::options::TunnelOptions;

use std::sync::Arc;

use crate::context::SessionContext;
use crate::planner::extension::{ExtensionNode, ExtensionType};
use crate::planner::logical_plan::CreateTunnel;
use crate::planner::physical_plan::CreateTunnelExec;

use super::client::RemoteSessionClient;

/// A planner that executes everything on a remote service.
#[derive(Debug, Clone)]
pub struct RemoteLogicalPlanner {
    /// Client to remote services.
    remote_client: RemoteSessionClient,
}

impl RemoteLogicalPlanner {
    pub fn new(client: RemoteSessionClient) -> RemoteLogicalPlanner {
        RemoteLogicalPlanner {
            remote_client: client,
        }
    }
}

#[async_trait]
impl QueryPlanner for RemoteLogicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
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
    /// Client to remote services.
    remote_client: RemoteSessionClient,
}

impl From<&RemoteLogicalPlanner> for RemotePhysicalPlanner {
    fn from(planner: &RemoteLogicalPlanner) -> Self {
        Self {
            remote_client: planner.remote_client.clone(),
        }
    }
}

#[async_trait]
impl PhysicalPlanner for RemotePhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut client = self.remote_client.clone();
        let physical_plan = client
            .create_physical_plan(logical_plan)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(physical_plan))
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

#[derive(Clone)]
pub struct CustomPhysicalPlanner<'a> {
    ctx: &'a SessionContext,
}

impl<'a> CustomPhysicalPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl<'a> PhysicalPlanner for CustomPhysicalPlanner<'a> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match logical_plan {
            LogicalPlan::Projection(_) => todo!("projection"),
            LogicalPlan::Filter(_) => todo!("filter"),
            LogicalPlan::Window(_) => todo!("window"),
            LogicalPlan::Aggregate(_) => todo!("aggregate"),
            LogicalPlan::Sort(_) => todo!("sort"),
            LogicalPlan::Join(_) => todo!(),
            LogicalPlan::CrossJoin(_) => todo!(),
            LogicalPlan::Repartition(_) => todo!(),
            LogicalPlan::Union(_) => todo!(),
            LogicalPlan::TableScan(_) => todo!("table scan"),
            LogicalPlan::EmptyRelation(_) => todo!(),
            LogicalPlan::Subquery(_) => todo!(),
            LogicalPlan::SubqueryAlias(_) => todo!(),
            LogicalPlan::Limit(_) => todo!(),
            LogicalPlan::Statement(_) => todo!(),
            LogicalPlan::Values(_) => todo!(),
            LogicalPlan::Explain(_) => todo!(),
            LogicalPlan::Analyze(_) => todo!(),
            LogicalPlan::Extension(extension) => {
                let node = extension.node.name().parse::<ExtensionType>().unwrap();
                match node {
                    ExtensionType::CreateTunnel => {
                        let create_tunnel = CreateTunnel::try_decode_extension(extension).unwrap();
                        let create_tunnel_exec = CreateTunnelExec {
                            name: create_tunnel.name,
                            if_not_exists: create_tunnel.if_not_exists,
                            options: create_tunnel.options,
                        };
                    }

                    _ => todo!(),
                }

                println!("extension: {:?}", extension.node);
            }
            LogicalPlan::Distinct(_) => todo!(),
            LogicalPlan::Prepare(_) => todo!(),
            LogicalPlan::Dml(_) => todo!(),
            LogicalPlan::Ddl(_) => todo!(),
            LogicalPlan::DescribeTable(_) => todo!(),
            LogicalPlan::Unnest(_) => todo!(),
        };

        todo!()
        // let physical_plan = client
        //     .create_physical_plan(logical_plan)
        //     .await
        //     .map_err(|e| DataFusionError::External(Box::new(e)))?;
        // Ok(Arc::new(physical_plan))
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
