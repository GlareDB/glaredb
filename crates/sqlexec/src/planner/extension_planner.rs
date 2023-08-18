use std::sync::Arc;

use datafusion::{
    execution::{context::SessionState, TaskContext},
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_ext::vars::SessionVars;

use crate::metastore::catalog::AsyncSessionCatalog;

use super::{extension::ExtensionType, physical_plan::create_table_exec::CreateTableExecPlanner};

pub struct GlareExtensionPlanner;

pub trait DatafusionExtensionProvider {
    fn get_session_catalog(&self) -> Arc<AsyncSessionCatalog>;
    fn get_session_vars(&self) -> SessionVars;
}

impl DatafusionExtensionProvider for &SessionState {
    fn get_session_catalog(&self) -> Arc<AsyncSessionCatalog> {
        self.config()
            .get_extension::<AsyncSessionCatalog>()
            .unwrap()
    }

    fn get_session_vars(&self) -> SessionVars {
        let vars = self
            .config_options()
            .extensions
            .get::<SessionVars>()
            .unwrap();
        vars.clone()
    }
}

impl DatafusionExtensionProvider for TaskContext {
    fn get_session_catalog(&self) -> Arc<AsyncSessionCatalog> {
        self.session_config()
            .get_extension::<AsyncSessionCatalog>()
            .unwrap()
    }

    fn get_session_vars(&self) -> SessionVars {
        self.session_config()
            .options()
            .extensions
            .get::<SessionVars>()
            .unwrap()
            .clone()
    }
}

#[async_trait::async_trait]
impl ExtensionPlanner for GlareExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let ext_type = node.name().parse::<ExtensionType>().unwrap();
        match ext_type {
            ExtensionType::CreateTable => {
                CreateTableExecPlanner
                    .plan_extension(
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        session_state,
                    )
                    .await
            }
            _ => todo!(),
        }
    }
}
