use glaredb_error::Result;

use crate::explain::node::{ExplainNode, ExplainedPlan};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_explain::BoundExplain;
use crate::logical::logical_explain::LogicalExplain;
use crate::logical::operator::{LocationRequirement, LogicalOperator, Node};
use crate::logical::planner::plan_query::QueryPlanner;
use crate::statistics::value::StatisticsValue;

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
                logical_unoptimized: ExplainedPlan::new_from_logical(
                    explain.verbose,
                    bind_context,
                    &plan,
                ),
                logical_optimized: None,
            },
            location: LocationRequirement::Any,
            children: vec![plan],
            estimated_cardinality: StatisticsValue::Unknown,
        }))
    }
}
