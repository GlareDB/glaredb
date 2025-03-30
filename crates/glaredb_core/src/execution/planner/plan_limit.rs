use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::limit::PhysicalLimit;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_limit::LogicalLimit;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_limit(
        &mut self,
        mut limit: Node<LogicalLimit>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = limit.location;
        let input = limit.take_one_child_exact()?;
        let child = self.plan(input)?;

        let operator = PhysicalLimit::new(
            limit.node.limit,
            limit.node.offset,
            child.operator.call_output_types(),
        );

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(self.id_gen.next_id(), operator),
            children: vec![child],
        })
    }
}
