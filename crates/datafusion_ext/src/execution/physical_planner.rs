use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use datafusion::{
    execution::context::{QueryPlanner, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

#[derive(Default, Clone, Copy)]
pub struct ErroringPlanner;

#[async_trait]
impl QueryPlanner for ErroringPlanner {
    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        panic!("what");
        Err(DataFusionError::Plan("Invalid query planner".to_string()))
    }
}

#[derive(Default)]
pub struct SessionQueryPlanner {
    /// Props that should be scoped to a single query execution.
    pub props: ExecutionProps,
}

#[async_trait]
impl QueryPlanner for SessionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SessionPhysicalPlanner::with_props(self.props.clone());
        let plan = planner
            .create_physical_plan(logical_plan, session_state)
            .await?;
        Ok(plan)
    }
}

pub struct SessionPhysicalPlanner {
    /// Datafusion's default physical planner.
    ///
    /// This has an implicity dependency on `config_options` and
    /// `physical_optimizers` (and probably more) from the session state.
    default: DefaultPhysicalPlanner,
    /// Props that should be scoped to a single query execution.
    props: ExecutionProps,
}

impl SessionPhysicalPlanner {
    fn with_props(props: ExecutionProps) -> SessionPhysicalPlanner {
        SessionPhysicalPlanner {
            default: DefaultPhysicalPlanner::default(),
            props,
        }
    }
}

#[async_trait]
impl PhysicalPlanner for SessionPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .default
            .create_physical_plan(logical_plan, session_state)
            .await?;
        Ok(plan)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(expr, input_dfschema, input_schema, &self.props)
    }
}
