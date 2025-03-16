use glaredb_error::Result;

use crate::explain::node::ExplainNode;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_explain::BoundExplain;
use crate::logical::logical_explain::LogicalExplain;
use crate::logical::operator::{LocationRequirement, LogicalOperator, Node};
use crate::logical::planner::plan_query::QueryPlanner;
use crate::logical::statistics::StatisticsValue;

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
                logical_unoptimized: ExplainNode::new_from_logical_plan(
                    bind_context,
                    explain.verbose,
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
