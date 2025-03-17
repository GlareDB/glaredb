use glaredb_error::{Result, not_implemented};

use super::OperatorPlanState;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_distinct::LogicalDistinct;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_distinct(
        &mut self,
        _distinct: Node<LogicalDistinct>,
    ) -> Result<PlannedOperatorWithChildren> {
        not_implemented!("plan distinct")
    }
}
