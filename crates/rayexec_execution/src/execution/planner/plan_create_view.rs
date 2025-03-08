use rayexec_error::Result;

use super::OperatorPlanState;
use crate::catalog::create::CreateViewInfo;
use crate::catalog::memory::MemoryCatalogTx;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_create::LogicalCreateView;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_create_view(
        &mut self,
        create: Node<LogicalCreateView>,
    ) -> Result<PlannedOperatorWithChildren> {
        let db = self.db_context.require_get_database(&create.node.catalog)?;

        let info = CreateViewInfo {
            name: create.node.name,
            column_aliases: create.node.column_aliases,
            on_conflict: create.node.on_conflict,
            query_string: create.node.query_string,
        };

        let tx = &MemoryCatalogTx {}; // TODO
        let operator = db.plan_create_view(tx, &create.node.schema, info)?;

        Ok(PlannedOperatorWithChildren {
            operator,
            children: Vec::new(),
        })
    }
}
