use rayexec_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::empty::PhysicalEmpty;
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_expression_list::LogicalExpressionList;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_expression_list(
        &mut self,
        mut list: Node<LogicalExpressionList>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = list.location;

        let input = list.take_one_child_exact()?;
        let table_refs = input.get_output_table_refs(self.bind_context);
        let child = self.plan(input)?;

        let rows = list
            .node
            .rows
            .into_iter()
            .map(|row| self.expr_planner.plan_scalars(&table_refs, &row))
            .collect::<Result<Vec<_>>>()?;

        let values = PhysicalValues::new(rows);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(values),
            children: vec![child],
        })
    }
}
