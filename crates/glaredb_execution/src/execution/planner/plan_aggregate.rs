use glaredb_error::{not_implemented, DbError, Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::hash_aggregate::{Aggregates, PhysicalHashAggregate};
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::ungrouped_aggregate::PhysicalUngroupedAggregate;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::expr::physical::PhysicalAggregateExpression;
use crate::expr::Expression;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_aggregate(
        &mut self,
        mut agg: Node<LogicalAggregate>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = agg.location;

        let input = agg.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        let child = self.plan(input)?;

        let mut preproject_exprs = Vec::new();
        // Place group by expressions in pre-projection and create column exprs
        // referencing those groups.
        let mut groups = Vec::new();
        for group_expr in agg.node.group_exprs {
            let scalar = self
                .expr_planner
                .plan_scalar(&input_refs, &group_expr)
                .context("Failed to plan expressions for group by pre-projection")?;

            let col_idx = preproject_exprs.len();
            let col_expr = PhysicalColumnExpr {
                idx: col_idx,
                datatype: scalar.datatype(),
            };

            preproject_exprs.push(scalar);
            groups.push(col_expr);
        }

        // Extract agg expression inputs as well, and place in pre-projection.
        let mut phys_aggs = Vec::new();
        for agg_expr in agg.node.aggregates {
            let start_col_index = preproject_exprs.len(); // Relative offset for preproject inputs.
            let agg = match agg_expr {
                Expression::Aggregate(agg) => agg,
                other => {
                    return Err(DbError::new(format!("Expected aggregate, got: {other}")));
                }
            };

            let mut agg_columns = Vec::with_capacity(agg.agg.state.inputs.len());

            for (rel_idx, arg) in agg.agg.state.inputs.iter().enumerate() {
                let scalar = self
                    .expr_planner
                    .plan_scalar(&input_refs, arg)
                    .context("Failed to plan expressions for aggregate pre-projection")?;

                let datatype = scalar.datatype();
                preproject_exprs.push(scalar);

                agg_columns.push(PhysicalColumnExpr {
                    idx: rel_idx + start_col_index,
                    datatype,
                });
            }

            let phys_agg = PhysicalAggregateExpression {
                function: agg.agg,
                columns: agg_columns,
                is_distinct: agg.distinct,
            };

            phys_aggs.push(phys_agg);
        }

        let child = PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(PhysicalProject::new(preproject_exprs)),
            children: vec![child],
        };

        if phys_aggs.iter().any(|agg| agg.is_distinct) {
            not_implemented!("distinct aggregates")
        }

        match agg.node.grouping_sets {
            Some(grouping_sets) => {
                // If we're working with groups, push a hash aggregate operator.
                let aggregates = Aggregates {
                    groups,
                    grouping_functions: agg.node.grouping_functions,
                    aggregates: phys_aggs,
                };
                let operator = PhysicalHashAggregate::new(aggregates, grouping_sets);

                Ok(PlannedOperatorWithChildren {
                    operator: PlannedOperator::new_execute(operator),
                    children: vec![child],
                })
            }
            None => {
                // Otherwise push an ungrouped aggregate operator.
                let operator = PhysicalUngroupedAggregate::new(phys_aggs);

                Ok(PlannedOperatorWithChildren {
                    operator: PlannedOperator::new_execute(operator),
                    children: vec![child],
                })
            }
        }
    }
}
