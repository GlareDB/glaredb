use glaredb_error::{Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::filter::PhysicalFilter;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_filter(
        &mut self,
        mut filter: Node<LogicalFilter>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = filter.location;

        let input = filter.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);

        let input = self.plan(input)?;
        let datatypes = input.operator.call_output_types();

        let predicate = self
            .expr_planner
            .plan_scalar(&input_refs, &filter.node.filter)
            .context("Failed to plan expressions for filter")?;

        let operator = PhysicalFilter {
            datatypes,
            predicate,
        };

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(operator),
            children: vec![input],
        })
    }
}
