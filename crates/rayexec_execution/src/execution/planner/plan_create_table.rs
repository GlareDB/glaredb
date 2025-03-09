use rayexec_error::{not_implemented, Result};

use super::OperatorPlanState;
use crate::catalog::create::CreateTableInfo;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_create::LogicalCreateTable;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_create_table(
        &mut self,
        create: Node<LogicalCreateTable>,
    ) -> Result<PlannedOperatorWithChildren> {
        let db = self.db_context.require_get_database(&create.node.catalog)?;

        if create.children.is_empty() {
            let info = CreateTableInfo {
                name: create.node.name,
                columns: create.node.columns,
                on_conflict: create.node.on_conflict,
            };

            let operator = db.plan_create_table(&create.node.schema, info)?;

            Ok(PlannedOperatorWithChildren {
                operator,
                children: Vec::new(),
            })
        } else {
            not_implemented!("ctas")
        }
    }
}
