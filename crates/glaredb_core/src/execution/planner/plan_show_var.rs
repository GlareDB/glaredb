use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::single_row::PhysicalSingleRow;
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
use crate::logical::logical_set::LogicalShowVar;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_show_var(
        &mut self,
        show: Node<LogicalShowVar>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = show.location;
        let show = show.into_inner();

        let operator = PhysicalValues::new(vec![vec![PhysicalLiteralExpr::new(show.value).into()]]);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(self.id_gen.next_id(), operator),
            children: vec![PlannedOperatorWithChildren {
                operator: PlannedOperator::new_pull(self.id_gen.next_id(), PhysicalSingleRow),
                children: Vec::new(),
            }],
        })
    }
}
