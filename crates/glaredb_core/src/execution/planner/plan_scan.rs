use glaredb_error::Result;

use super::OperatorPlanState;
use crate::execution::operators::scan::PhysicalScan;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_scan::LogicalScan;
use crate::logical::operator::Node;
use crate::runtime::system::SystemRuntime;
use crate::storage::projections::Projections;

impl<R> OperatorPlanState<'_, R>
where
    R: SystemRuntime,
{
    pub fn plan_scan(&mut self, scan: Node<LogicalScan>) -> Result<PlannedOperatorWithChildren> {
        let _location = scan.location;

        let projections = match scan.node.meta_scan {
            Some(meta_scan) => {
                Projections::new_with_meta(scan.node.data_scan.projection, meta_scan.projection)
            }
            None => Projections::new(scan.node.data_scan.projection),
        };
        // TODO: Chain metadata
        //
        // Current not having metadata filters here is fine since we will have a
        // filter node above this anyways. Obviously being able to filter on
        // metadata columns is good, so needs to happen soon...
        let filters = scan
            .node
            .data_scan
            .scan_filters
            .into_iter()
            .map(|filter| {
                filter.plan(
                    scan.node.data_scan.table_ref,
                    self.bind_context,
                    &projections,
                    &self.expr_planner,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let function = scan.node.source.into_function();

        let operator = PhysicalScan::new(projections, filters, function);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_pull::<_, R>(self.id_gen.next_id(), operator),
            children: Vec::new(),
        })
    }
}
