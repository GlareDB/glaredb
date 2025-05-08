use glaredb_error::{DbError, Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::hash_aggregate::{Aggregates, PhysicalHashAggregate};
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
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

        // Generate distinct exprs from the projection, added to the hash
        // aggregate after the projection to preserve set semantics.
        let distinct_exprs: Vec<_> = projections
            .iter()
            .enumerate()
            .map(|(idx, proj)| PhysicalColumnExpr::new(idx, proj.datatype()))
            .collect();

        let proj_op = PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(
                self.id_gen.next_id(),
                PhysicalProject::new(projections),
            ),
            children: vec![scan_op],
        };

        // Add a distinct operator...
        let grouping_set = (0..distinct_exprs.len()).collect();
        let aggregates = Aggregates {
            groups: distinct_exprs,
            grouping_functions: Vec::new(),
            aggregates: Vec::new(),
        };
        // Ouput has GROUP_VALS first, so the projection ordering is preserved.
        let agg = PhysicalHashAggregate::new(aggregates, vec![grouping_set]);

        let agg_op = PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(self.id_gen.next_id(), agg),
            children: vec![proj_op],
        };

        Ok(agg_op)
    }
}
