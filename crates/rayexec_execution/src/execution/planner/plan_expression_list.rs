use rayexec_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::empty::PhysicalEmpty;
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_expression_list::LogicalExpressionList;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_expression_list(
        &mut self,
        list: Node<LogicalExpressionList>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = list.location;

        // TODO: Children, table refs.

        let rows = list
            .node
            .rows
            .into_iter()
            .map(|row| self.expr_planner.plan_scalars(&[], &row))
            .collect::<Result<Vec<_>>>()?;

        let values = PhysicalValues::new(rows);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(values),
            children: vec![PlannedOperatorWithChildren {
                operator: PlannedOperator::new_pull(PhysicalEmpty),
                children: Vec::new(),
            }],
        })
    }
}
