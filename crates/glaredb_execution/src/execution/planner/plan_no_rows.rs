use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::no_rows::PhysicalNoRows;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_no_rows::LogicalNoRows;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_no_rows(
        &mut self,
        node: Node<LogicalNoRows>,
    ) -> Result<PlannedOperatorWithChildren> {
        let mut datatypes = Vec::new();
        for table_ref in node.get_output_table_refs(self.bind_context) {
            let table = self.bind_context.get_table(table_ref)?;
            datatypes.extend_from_slice(&table.column_types);
        }

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_pull(
                self.id_gen.next_id(),
                PhysicalNoRows { datatypes },
            ),
            children: Vec::new(),
        })
    }
}
