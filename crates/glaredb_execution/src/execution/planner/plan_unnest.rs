use glaredb_error::{not_implemented, Result};

use super::OperatorPlanState;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_unnest::LogicalUnnest;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_unnest(
        &mut self,
        _unnest: Node<LogicalUnnest>,
    ) -> Result<PlannedOperatorWithChildren> {
        not_implemented!("plan unnest")
    }
}
