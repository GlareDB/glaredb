use std::sync::Arc;

use datafusion::{
    common::{DataFusionError, Result, ToDFSchema},
    logical_expr::{Analyze, Explain, LogicalPlan, PlanType, ToStringifiedPlan},
    sql::sqlparser::ast::Statement,
};

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    pub async fn explain_statement_to_plan(
        &mut self,
        verbose: bool,
        analyze: bool,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        let plan = match statement {
            Statement::Query(query) => self.query_to_plan(*query).await?,
            x => {
                return Err(DataFusionError::Plan(format!(
                    "Explain only supported for Query, not '{x}'"
                )))
            }
        };
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];
            Ok(LogicalPlan::Explain(Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            }))
        }
    }
}
