use rayexec_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::sort::PhysicalSort;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_order::LogicalOrder;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_sort(
        &mut self,
        mut order: Node<LogicalOrder>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = order.location;

        let input = order.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        let child = self.plan(input)?;

        let sort_exprs = self
            .expr_planner
            .plan_sorts(&input_refs, &order.node.exprs)?;

        let sort = PhysicalSort::new(sort_exprs, child.operator.call_output_types());

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(sort),
            children: vec![child],
        })
    }
}
