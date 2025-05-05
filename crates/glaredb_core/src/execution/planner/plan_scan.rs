use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::scan::PhysicalScan;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_scan::LogicalScan;
use crate::logical::operator::Node;
use crate::storage::projections::Projections;

impl OperatorPlanState<'_> {
    pub fn plan_scan(&mut self, scan: Node<LogicalScan>) -> Result<PlannedOperatorWithChildren> {
        let _location = scan.location;

        let projections = Projections::new(scan.node.projection);
        let filters = scan
            .node
            .scan_filters
            .into_iter()
            .map(|filter| filter.plan(scan.node.table_ref, self.bind_context, &self.expr_planner))
            .collect::<Result<Vec<_>>>()?;

        let function = scan.node.source.into_function();

        let operator = PhysicalScan::new(projections, filters, function);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_pull(self.id_gen.next_id(), operator),
            children: Vec::new(),
        })
    }
}
