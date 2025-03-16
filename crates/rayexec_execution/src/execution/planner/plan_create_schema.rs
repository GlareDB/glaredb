use rayexec_error::Result;

use super::OperatorPlanState;
use crate::catalog::create::CreateSchemaInfo;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_create::LogicalCreateSchema;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_create_schema(
        &mut self,
        create: Node<LogicalCreateSchema>,
    ) -> Result<PlannedOperatorWithChildren> {
        let db = self.db_context.require_get_database(&create.node.catalog)?;

        let info = CreateSchemaInfo {
            name: create.node.name,
            on_conflict: create.node.on_conflict,
        };

        let operator = db.plan_create_schema(info)?;

        Ok(PlannedOperatorWithChildren {
            operator,
            children: Vec::new(),
        })
    }
}
