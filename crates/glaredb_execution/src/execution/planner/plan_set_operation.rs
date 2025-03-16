use glaredb_error::{not_implemented, Result};

use super::OperatorPlanState;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_setop::LogicalSetop;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_set_operation(
        &mut self,
        _setop: Node<LogicalSetop>,
    ) -> Result<PlannedOperatorWithChildren> {
        not_implemented!("plan set operation")
    }
}
