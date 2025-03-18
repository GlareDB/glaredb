use glaredb_error::{Result, not_implemented};

use super::OperatorPlanState;
use crate::execution::operators::hash_aggregate::{Aggregates, PhysicalHashAggregate};
use crate::execution::operators::union::PhysicalUnion;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::logical::logical_setop::{LogicalSetop, SetOpKind};
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_set_operation(
        &mut self,
        mut setop: Node<LogicalSetop>,
    ) -> Result<PlannedOperatorWithChildren> {
        let [left, right] = setop.take_two_children_exact()?;
        let left = self.plan(left)?;
        let right = self.plan(right)?;

        let left_types = left.operator.call_output_types();

        let mut operator = match setop.node.kind {
            SetOpKind::Union => {
                let operator = PhysicalUnion::new(left_types);
                PlannedOperatorWithChildren {
                    operator: PlannedOperator::new_push_execute(operator),
                    children: vec![left, right],
                }
            }
            other => not_implemented!("set operation: {other}"),
        };

        if !setop.node.all {
            // Make output distinct on all columns.
            let output_types = operator.operator.call_output_types();
            let grouping_set = vec![(0..output_types.len()).collect()];

            let groups = output_types
                .into_iter()
                .enumerate()
                .map(|(col_idx, datatype)| PhysicalColumnExpr {
                    idx: col_idx,
                    datatype,
                })
                .collect();

            let aggregates = Aggregates {
                groups,
                grouping_functions: Vec::new(),
                aggregates: Vec::new(),
            };

            operator = PlannedOperatorWithChildren {
                operator: PlannedOperator::new_execute(PhysicalHashAggregate::new(
                    aggregates,
                    grouping_set,
                )),
                children: vec![operator],
            };
        }

        Ok(operator)
    }
}
