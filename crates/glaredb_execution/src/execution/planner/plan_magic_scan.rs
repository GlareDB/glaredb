use glaredb_error::{DbError, Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_materialization::LogicalMagicMaterializationScan;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_magic_materialize_scan(
        &mut self,
        scan: Node<LogicalMagicMaterializationScan>,
    ) -> Result<PlannedOperatorWithChildren> {
        let mat_op = self.materializations.get(&scan.node.mat).ok_or_else(|| {
            DbError::new(format!(
                "Missing materialization for ref: {}",
                scan.node.mat
            ))
        })?;

        // As with a normal materialized scan, we just need the operator, skip
        // the children.
        let scan_op = PlannedOperatorWithChildren {
            operator: mat_op.operator.clone(),
            children: Vec::new(),
        };

        // Plan the projection out of the materialization.
        let materialized_refs = &self
            .bind_context
            .get_materialization(scan.node.mat)?
            .table_refs;
        let projections = self
            .expr_planner
            .plan_scalars(materialized_refs, &scan.node.projections)
            .context("Failed to plan projections out of materialization")?;

        let proj_op = PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(PhysicalProject::new(projections)),
            children: vec![scan_op],
        };

        // TODO: Distinct the projection.

        Ok(proj_op)
    }
}
