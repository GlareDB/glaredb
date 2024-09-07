use crate::logical::{
    binder::{bind_context::BindContext, bind_explain::BoundExplain},
    logical_explain::LogicalExplain,
    operator::{LocationRequirement, LogicalOperator, Node},
    planner::plan_query::QueryPlanner,
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct ExplainPlanner;

impl ExplainPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        explain: BoundExplain,
    ) -> Result<LogicalOperator> {
        let plan = QueryPlanner.plan(bind_context, explain.query)?;

        Ok(LogicalOperator::Explain(Node {
            node: LogicalExplain {
                analyze: explain.analyze,
                verbose: explain.verbose,
                format: explain.format,
                logical_unoptimized: Box::new(plan.clone()),
                logical_optimized: None,
            },
            location: LocationRequirement::Any,
            children: vec![plan],
        }))
    }
}
