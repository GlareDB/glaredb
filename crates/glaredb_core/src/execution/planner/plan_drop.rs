use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_drop::LogicalDrop;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_drop(&mut self, drop: Node<LogicalDrop>) -> Result<PlannedOperatorWithChildren> {
        let _location = drop.location;

        let db = self.db_context.require_get_database(&drop.node.catalog)?;
        let operator = db.plan_drop(&mut self.id_gen, drop.node.info)?;

        Ok(PlannedOperatorWithChildren {
            operator,
            children: Vec::new(),
        })
    }
}
