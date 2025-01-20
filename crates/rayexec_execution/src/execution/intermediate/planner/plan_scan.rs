use std::sync::Arc;

use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};

use super::{InProgressPipeline, IntermediatePipelineBuildState, PipelineIdGen};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::execution::intermediate::pipeline::{IntermediateOperator, PipelineSource};
use crate::execution::operators::scan::PhysicalScan;
use crate::execution::operators::table_function::PhysicalTableFunction;
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::PhysicalOperator;
use crate::expr::Expression;
use crate::logical::logical_scan::{LogicalScan, ScanSource};
use crate::logical::operator::Node;
use crate::storage::table_storage::Projections;

impl IntermediatePipelineBuildState<'_> {
    #[allow(deprecated)]
    pub fn plan_scan(&mut self, id_gen: &mut PipelineIdGen, scan: Node<LogicalScan>) -> Result<()> {
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

        let operator = match scan.node.source {
            ScanSource::Table {
                catalog,
                schema,
                source,
            } => IntermediateOperator {
                operator: Arc::new(PhysicalOperator::Scan(PhysicalScan::new(
                    catalog,
                    schema,
                    source,
                    projections,
                ))),
                partitioning_requirement: None,
            },
            ScanSource::TableFunction { function } => IntermediateOperator {
                operator: Arc::new(PhysicalOperator::TableFunction(PhysicalTableFunction::new(
                    function,
                    projections,
                ))),
                partitioning_requirement: None,
            },
            ScanSource::ExpressionList { rows } => {
                // let batches = self.create_batches_for_row_values(projections, rows)?;
                // TODO: Table refs
                unimplemented!()
                // let expressions = rows.into_iter().map(|row| {
                //     self.expr_planner.plan_scalars(table_refs, exprs)
                // })
                // IntermediateOperator {
                //     operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(batches))),
                //     partitioning_requirement: None,
                // }
            }
            ScanSource::View { .. } => not_implemented!("view physical planning"),
        };

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
