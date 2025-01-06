use std::sync::Arc;

use rayexec_error::{RayexecError, Result, ResultExt};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::hash_aggregate::PhysicalHashAggregate;
use crate::execution::operators::project::{PhysicalProject2, ProjectOperation};
use crate::execution::operators::ungrouped_aggregate::PhysicalUngroupedAggregate;
use crate::execution::operators::PhysicalOperator;
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::expr::physical::PhysicalAggregateExpression;
use crate::expr::Expression;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::operator::{LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_aggregate(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut agg: Node<LogicalAggregate>,
    ) -> Result<()> {
        let location = agg.location;

        let input = agg.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        let mut phys_aggs = Vec::new();

        // Extract agg expressions, place in their own pre-projection.
        let mut preproject_exprs = Vec::new();
        for agg_expr in agg.node.aggregates {
            let agg = match agg_expr {
                Expression::Aggregate(agg) => agg,
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected aggregate, got: {other}"
                    )))
                }
            };

            for arg in &agg.agg.inputs {
                let scalar = self
                    .expr_planner
                    .plan_scalar(&input_refs, arg)
                    .context("Failed to plan expressions for aggregate pre-projection")?;
                preproject_exprs.push(scalar);
            }

            let columns = preproject_exprs
                .iter()
                .enumerate()
                .map(|(idx, expr)| PhysicalColumnExpr {
                    idx,
                    datatype: expr.datatype(),
                })
                .collect();

            let phys_agg = PhysicalAggregateExpression {
                function: agg.agg,
                columns,
                is_distinct: agg.distinct,
            };

            phys_aggs.push(phys_agg);
        }

        // Place group by expressions in pre-projection as well.
        for group_expr in agg.node.group_exprs {
            let scalar = self
                .expr_planner
                .plan_scalar(&input_refs, &group_expr)
                .context("Failed to plan expressions for group by pre-projection")?;

            preproject_exprs.push(scalar);
        }

        // // Resize batches prior to pre-projection.
        // self.push_batch_resizer(id_gen)?;

        self.push_intermediate_operator(
            IntermediateOperator {
                operator: Arc::new(PhysicalOperator::Project(PhysicalProject2 {
                    operation: ProjectOperation::new(preproject_exprs),
                })),
                partitioning_requirement: None,
            },
            location,
            id_gen,
        )?;

        match agg.node.grouping_sets {
            Some(grouping_sets) => {
                // If we're working with groups, push a hash aggregate operator.
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HashAggregate(
                        PhysicalHashAggregate::new(
                            phys_aggs,
                            grouping_sets,
                            agg.node.grouping_functions,
                        ),
                    )),
                    partitioning_requirement: None,
                };
                self.push_intermediate_operator(operator, location, id_gen)?;
            }
            None => {
                // Otherwise push an ungrouped aggregate operator.

                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::UngroupedAggregate(
                        PhysicalUngroupedAggregate::new(phys_aggs),
                    )),
                    partitioning_requirement: None,
                };
                self.push_intermediate_operator(operator, location, id_gen)?;
            }
        };

        Ok(())
    }
}
