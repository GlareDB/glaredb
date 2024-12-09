use std::sync::Arc;

use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};

use super::{InProgressPipeline, IntermediatePipelineBuildState, PipelineIdGen};
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
                let batches = self.create_batches_for_row_values(projections, rows)?;
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(batches))),
                    partitioning_requirement: None,
                }
            }
            ScanSource::View { .. } => not_implemented!("view physical planning"),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn create_batches_for_row_values(
        &self,
        projections: Projections,
        rows: Vec<Vec<Expression>>,
    ) -> Result<Vec<Batch>> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // TODO: This could probably be simplified.

        let mut row_arrs: Vec<Vec<Array>> = Vec::new(); // Row oriented.
        let dummy_batch = Batch::empty_with_num_rows(1);

        // Convert expressions into arrays of one element each.
        for row_exprs in rows {
            let exprs = self
                .expr_planner
                .plan_scalars(&[], &row_exprs)
                .context("Failed to plan expressions for values")?;
            let arrs = exprs
                .into_iter()
                .map(|expr| {
                    let arr = expr.eval(&dummy_batch)?;
                    Ok(arr.into_owned())
                })
                .collect::<Result<Vec<_>>>()?;
            row_arrs.push(arrs);
        }

        let batches = row_arrs
            .into_iter()
            .map(|cols| {
                let batch = Batch::try_new(cols)?;

                // TODO: Got lazy, we can just avoid evaluating the expressions above.
                match &projections.column_indices {
                    Some(indices) => Ok(batch.project(indices)),
                    None => Ok(batch),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(batches)
    }
}
