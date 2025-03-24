use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::single_row::PhysicalSingleRow;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_single_row::LogicalSingleRow;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_single_row(
        &mut self,
        _: Node<LogicalSingleRow>,
    ) -> Result<PlannedOperatorWithChildren> {
        // "Empty" is a source of data by virtue of emitting a batch consisting
        // of no columns and 1 row.
        //
        // This enables expression evualtion to work without needing to special
        // case a query without a FROM clause. E.g. `SELECT 1+1` would execute
        // the expression `1+1` with the input being the batch with 1 row and no
        // columns.
        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_pull(self.id_gen.next_id(), PhysicalSingleRow),
            children: Vec::new(),
        })
    }
}
