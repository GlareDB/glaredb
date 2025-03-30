use glaredb_error::{DbError, Result};

use super::OperatorPlanState;
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_materialization::LogicalMaterializationScan;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_materialize_scan(
        &mut self,
        scan: Node<LogicalMaterializationScan>,
    ) -> Result<PlannedOperatorWithChildren> {
        let mat_op = self.materializations.get(&scan.node.mat).ok_or_else(|| {
            DbError::new(format!(
                "Missing materialization for ref: {}",
                scan.node.mat
            ))
        })?;

        // Scan/pull operator. Clone only the operator itself and skip the
        // children.
        Ok(PlannedOperatorWithChildren {
            operator: mat_op.operator.clone(),
            children: Vec::new(),
        })
    }
}
