use rayexec_error::{RayexecError, Result};

use super::OperatorPlanState;
use crate::logical::logical_scan::LogicalScan;
use crate::logical::operator::Node;
use crate::storage::table_storage::Projections;

impl OperatorPlanState<'_> {
    #[allow(deprecated)]
    pub fn plan_scan(&mut self, scan: Node<LogicalScan>) -> Result<()> {
        let location = scan.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // TODO: Split up scan source.
        let projections = if scan.node.did_prune_columns {
            Projections {
                column_indices: Some(scan.node.projection),
            }
        } else {
            Projections::all()
        };

        // let operator = match scan.node.source {
        //     ScanSource::Table {
        //         catalog,
        //         schema,
        //         source,
        //     } => PhysicalOperator::Scan(PhysicalScan::new(catalog, schema, source, projections)),
        //     ScanSource::TableFunction { function } => {
        //         PhysicalOperator::TableFunction(PhysicalTableFunction::new(function, projections))
        //     }
        //     ScanSource::ExpressionList { rows } => {
        //         // let batches = self.create_batches_for_row_values(projections, rows)?;
        //         // TODO: Table refs
        //         unimplemented!()
        //         // let expressions = rows.into_iter().map(|row| {
        //         //     self.expr_planner.plan_scalars(table_refs, exprs)
        //         // })
        //         // IntermediateOperator {
        //         //     operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(batches))),
        //         //     partitioning_requirement: None,
        //         // }
        //     }
        //     ScanSource::View { .. } => not_implemented!("view physical planning"),
        // };

        unimplemented!()
        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: vec![operator],
        //     location,
        //     source: PipelineSource::InPipeline,
        // });

        // Ok(())
    }
}
