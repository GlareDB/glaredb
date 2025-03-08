use rayexec_error::{not_implemented, Result};

use super::OperatorPlanState;
use crate::execution::operators::scan::PhysicalScan;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_scan::{LogicalScan, ScanSource};
use crate::logical::operator::Node;
use crate::storage::projections::Projections;

impl OperatorPlanState<'_> {
    pub fn plan_scan(&mut self, scan: Node<LogicalScan>) -> Result<PlannedOperatorWithChildren> {
        let _location = scan.location;

        let projections = Projections::new(scan.node.projection);

        match scan.node.source {
            ScanSource::Table(_) => not_implemented!("table scan"), // TODO: Use table function?
            ScanSource::Function(source) => {
                let operator = PhysicalScan::new(projections, source.function);

                Ok(PlannedOperatorWithChildren {
                    operator: PlannedOperator::new_pull(operator),
                    children: Vec::new(),
                })
            }
        }
    }
}
