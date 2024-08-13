#![allow(clippy::single_match)] // Easier to read for this rule.

use std::collections::HashMap;

use crate::{
    expr::scalar::{BinaryOperator, PlannedBinaryOperator},
    logical::{
        expr::LogicalExpression,
        operator::{EqualityJoin, LogicalNode, LogicalOperator, Projection},
    },
};
use rayexec_error::{RayexecError, Result};

use super::OptimizeRule;

#[derive(Debug, Clone)]
pub struct JoinOrderRule {}

impl OptimizeRule for JoinOrderRule {
    fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.optimize_any_join_to_equality_join(plan)
    }
}

impl JoinOrderRule {
    /// Try to swap out any joins (joins with arbitrary expressions) with
    /// equality joins.
    fn optimize_any_join_to_equality_join(
        &self,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        plan.walk_mut_post(&mut |plan| {
            match plan {
                LogicalOperator::AnyJoin(join) => {
                    let join = join.as_mut();

                    // Used to adjust the indexes used for the on keys.
                    let left_len = join.left.output_schema(&[])?.types.len();
                    let right_len = join.right.output_schema(&[])?.types.len();

                    // Extract constants, place in pre-projection.
                    //
                    // Pre-projection will be applied to right, so column
                    // indices will reflect that.
                    let mut constants = Vec::new();
                    join.on.walk_mut_post(&mut |expr| {
                        if expr.is_constant() {
                            let new_expr = LogicalExpression::new_column(
                                left_len + right_len + constants.len(),
                            );
                            let constant = std::mem::replace(expr, new_expr);
                            constants.push(constant);
                        }
                        Ok(())
                    })?;

                    // Replace right if constants were extracted.
                    //
                    // Even if we don't end up replacing the join, this
                    // pre-projection is still valid.
                    if !constants.is_empty() {
                        let exprs = (0..right_len)
                            .map(LogicalExpression::new_column)
                            .chain(constants.into_iter())
                            .collect();

                        let orig = join.right.take_boxed();
                        let projection = Projection { exprs, input: orig };

                        join.right =
                            Box::new(LogicalOperator::Projection(LogicalNode::new(projection)));
                    }

                    let mut conjunctives = Vec::with_capacity(1);
                    split_conjunctive(join.on.clone(), &mut conjunctives);

                    let mut left_on = Vec::new();
                    let mut right_on = Vec::new();

                    let mut remaining = Vec::new();
                    for expr in conjunctives {
                        // Currently this just does a basic 'col1 = col2' check.
                        match &expr {
                            LogicalExpression::Binary {
                                op:
                                    PlannedBinaryOperator {
                                        op: BinaryOperator::Eq,
                                        ..
                                    },
                                left,
                                right,
                            } => {
                                match (left.as_ref(), right.as_ref()) {
                                    (
                                        LogicalExpression::ColumnRef(left),
                                        LogicalExpression::ColumnRef(right),
                                    ) => {
                                        if let (Ok(left), Ok(right)) = (
                                            left.try_as_uncorrelated(),
                                            right.try_as_uncorrelated(),
                                        ) {
                                            // If correlated, then this would be a
                                            // lateral join. Unsure how we want to
                                            // optimize that right now.

                                            // Normal 'left_table_col = right_table_col'
                                            if left < left_len && right >= left_len {
                                                left_on.push(left);
                                                right_on.push(right - left_len);
                                                // This expression was handled, avoid
                                                // putting it in remaining.
                                                continue;
                                            }

                                            // May be flipped like 'right_table_col = left_table_col'
                                            if right < left_len && left >= left_len {
                                                left_on.push(right);
                                                right_on.push(left - left_len);
                                                // This expression was handled, avoid
                                                // putting it in remaining.
                                                continue;
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            _ => (),
                        }

                        // Didn't handle this expression. Add it to remaining.
                        remaining.push(expr);
                    }

                    // We were able to extract all equalities. Update the plan
                    // to be an equality join.
                    if remaining.is_empty() {
                        // TODO: Should use location from original join.
                        *plan = LogicalOperator::EqualityJoin(LogicalNode::new(EqualityJoin {
                            left: join.left.take_boxed(),
                            right: join.right.take_boxed(),
                            join_type: join.join_type,
                            left_on,
                            right_on,
                        }));
                    }

                    Ok(())
                }
                _ => Ok(()), // Not a plan we can optimize.
            }
        })?;

        Ok(plan)
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
struct InputPreprojections {
    /// The new ON expression to use.
    on_expr: LogicalExpression,

    /// _Additional_ projections that should be added to the left input. If no
    /// projection is needed, this will be empty.
    left_projections: Vec<LogicalExpression>,

    /// _Additional_ projections that should be added to the right input. If no
    /// projection is needed, this will be empty.
    right_projections: Vec<LogicalExpression>,
}

#[allow(dead_code)]
impl InputPreprojections {
    /// Try to compute input pre-projections from an ON expression from a join.
    ///
    /// This accepts `left_len` and `right_len` which should correspond to the
    /// size of the left and right inputs. These values are used computing the
    /// new column references.
    fn try_compute_from_on_expr(
        left_len: usize,
        right_len: usize,
        mut on_expr: LogicalExpression,
    ) -> Result<Self> {
        // 1. Walks the expression, moves pre-projections into essentially a
        //    third input.
        // 2. Compute the number pre-projections for left and right.
        // 3. Walks expression again and updates the column references
        //    to point to the correct columns.

        #[derive(Debug, PartialEq)]
        enum InputSide {
            Left,
            Right,
        }

        let mut preprojections: HashMap<usize, (LogicalExpression, InputSide)> = HashMap::new();

        let maybe_swap_projection = &mut |expr: &mut LogicalExpression| {
            let idx = left_len + right_len + preprojections.len();

            // No need to change column references.
            if matches!(expr, LogicalExpression::ColumnRef(_)) {
                return Ok::<_, RayexecError>(false);
            }

            // Maybe left.
            if expr_within_relation_bound(expr, 0, left_len - 1) {
                let new_expr = LogicalExpression::new_column(idx);
                let orig = std::mem::replace(expr, new_expr);
                preprojections.insert(idx, (orig, InputSide::Left));
                return Ok(true);
            }

            // Maybe right.
            if expr_within_relation_bound(expr, left_len, left_len + right_len - 1) {
                let new_expr = LogicalExpression::new_column(idx);
                let mut orig = std::mem::replace(expr, new_expr);

                // Update original expression to be relative to the projection.
                orig.walk_mut_post(&mut |expr| match expr {
                    LogicalExpression::ColumnRef(column) => {
                        if column.scope_level != 0 {
                            return Err(RayexecError::new("unhandled scope level"));
                        }
                        column.item_idx -= left_len;
                        Ok(())
                    }
                    _ => Ok(()),
                })?;

                preprojections.insert(idx, (orig, InputSide::Right));
                return Ok(true);
            }

            // All constants go to right.
            if expr.is_constant() {
                let new_expr = LogicalExpression::new_column(idx);
                let orig = std::mem::replace(expr, new_expr);
                preprojections.insert(idx, (orig, InputSide::Right));
                return Ok(true);
            }

            Ok(false)
        };

        on_expr.walk_mut_pre(&mut |expr| {
            maybe_swap_projection(expr)?;
            Ok(())
        })?;

        let left_projection_count = preprojections
            .values()
            .filter(|(_, side)| matches!(side, InputSide::Left))
            .count();

        let right_projection_count = preprojections
            .values()
            .filter(|(_, side)| matches!(side, InputSide::Right))
            .count();

        let mut left_projections = Vec::with_capacity(left_projection_count);
        let mut right_projections = Vec::with_capacity(right_projection_count);

        on_expr.walk_mut_post(&mut |expr| {
            match expr {
                LogicalExpression::ColumnRef(column_ref) => {
                    if column_ref.scope_level != 0 {
                        // TODO: Unsure.
                        return Ok(());
                    }

                    match preprojections.remove(&column_ref.item_idx) {
                        Some((expr, InputSide::Left)) => {
                            column_ref.item_idx = left_len + left_projections.len();
                            left_projections.push(expr);
                        }
                        Some((expr, InputSide::Right)) => {
                            column_ref.item_idx = left_len
                                + left_projection_count
                                + right_len
                                + right_projections.len();
                            right_projections.push(expr);
                        }
                        None => {
                            // Column references that were in the original
                            // expression.
                            //
                            // - Left references remain valid.
                            // - Right references need to be updated to account
                            //   for added left projections. This can be done by
                            //   just adding the number of additional left
                            //   projection to the original reference.
                            if column_ref.item_idx >= left_len
                                && column_ref.item_idx < (left_len + right_len)
                            {
                                column_ref.item_idx += left_projection_count;
                            }
                        }
                    }
                }
                _ => (),
            }
            Ok(())
        })?;

        Ok(InputPreprojections {
            on_expr,
            left_projections,
            right_projections,
        })
    }
}

/// Split a logical expression on AND conditions, appending
/// them to the provided vector.
fn split_conjunctive(expr: LogicalExpression, outputs: &mut Vec<LogicalExpression>) {
    match expr {
        LogicalExpression::Binary {
            op:
                PlannedBinaryOperator {
                    op: BinaryOperator::And,
                    ..
                },
            left,
            right,
        } => {
            split_conjunctive(*left, outputs);
            split_conjunctive(*right, outputs);
        }
        other => outputs.push(other),
    }
}

fn expr_within_relation_bound(expr: &LogicalExpression, min: usize, max: usize) -> bool {
    match expr {
        LogicalExpression::ColumnRef(col) => {
            if col.scope_level != 0 {
                return false;
            }
            col.item_idx >= min && col.item_idx <= max
        }
        LogicalExpression::Literal(_) => false,
        LogicalExpression::ScalarFunction { inputs, .. } => inputs
            .iter()
            .all(|expr| expr_within_relation_bound(expr, min, max)),
        LogicalExpression::Cast { expr, .. } => expr_within_relation_bound(expr.as_ref(), min, max),
        LogicalExpression::Unary { expr, .. } => {
            expr_within_relation_bound(expr.as_ref(), min, max)
        }
        LogicalExpression::Binary { left, right, .. } => {
            expr_within_relation_bound(left.as_ref(), min, max)
                && expr_within_relation_bound(right.as_ref(), min, max)
        }
        LogicalExpression::Variadic { exprs, .. } => exprs
            .iter()
            .all(|expr| expr_within_relation_bound(expr, min, max)),
        LogicalExpression::Aggregate { inputs, .. } => inputs
            .iter()
            .all(|expr| expr_within_relation_bound(expr, min, max)),
        LogicalExpression::Subquery(_) => false,
        LogicalExpression::Case { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataType;

    use crate::functions::scalar::comparison::EqImpl;

    use similar_asserts::assert_eq;

    use super::*;

    fn new_binary_eq() -> PlannedBinaryOperator {
        PlannedBinaryOperator {
            op: BinaryOperator::Eq,
            scalar: Box::new(EqImpl),
        }
    }

    #[test]
    fn input_preprojections_no_update_needed() {
        let on_expr = LogicalExpression::Binary {
            op: new_binary_eq(),
            left: Box::new(LogicalExpression::new_column(2)),
            right: Box::new(LogicalExpression::new_column(3)),
        };

        let preprojections = InputPreprojections::try_compute_from_on_expr(3, 2, on_expr).unwrap();

        let expected = InputPreprojections {
            on_expr: LogicalExpression::Binary {
                op: new_binary_eq(),
                left: Box::new(LogicalExpression::new_column(2)),
                right: Box::new(LogicalExpression::new_column(3)),
            },
            left_projections: Vec::new(),
            right_projections: Vec::new(),
        };

        assert_eq!(expected, preprojections)
    }

    #[test]
    fn input_preprojections_cast_left_input_unchanged_right() {
        let on_expr = LogicalExpression::Binary {
            op: new_binary_eq(),
            left: Box::new(LogicalExpression::Cast {
                to: DataType::Float64,
                expr: Box::new(LogicalExpression::new_column(2)),
            }),
            right: Box::new(LogicalExpression::new_column(3)),
        };

        let preprojections = InputPreprojections::try_compute_from_on_expr(3, 2, on_expr).unwrap();

        let expected = InputPreprojections {
            on_expr: LogicalExpression::Binary {
                op: new_binary_eq(),
                // Additional left projection added to end (3 + 1)
                left: Box::new(LogicalExpression::new_column(4)),
                // Right needs to account for additional left (3 + 1 + 1)
                right: Box::new(LogicalExpression::new_column(5)),
            },
            left_projections: vec![LogicalExpression::Cast {
                to: DataType::Float64,
                expr: Box::new(LogicalExpression::new_column(2)),
            }],
            right_projections: Vec::new(),
        };

        assert_eq!(expected, preprojections)
    }

    // #[test]
    // fn input_preprojections_cast_right_input_unchanged_left() {
    //     let on_expr = LogicalExpression::Binary {
    //         op: new_binary_eq(),
    //         left: Box::new(LogicalExpression::new_column(2)),
    //         right: Box::new(LogicalExpression::Cast {
    //             to: DataType::Float64,
    //             expr: Box::new(LogicalExpression::new_column(3)),
    //         }),
    //     };

    //     let preprojections = InputPreprojections::try_compute_from_on_expr(3, 2, on_expr).unwrap();

    //     let expected = InputPreprojections {
    //         on_expr: LogicalExpression::Binary {
    //             op: new_binary_eq(),
    //             // No change needed.
    //             left: Box::new(LogicalExpression::new_column(2)),
    //             // Righ now points to the appended column on the right side.
    //             right: Box::new(LogicalExpression::new_column(5)),
    //         },
    //         left_projections: Vec::new(),
    //         right_projections: vec![LogicalExpression::Cast {
    //             to: DataType::Float64,
    //             expr: Box::new(LogicalExpression::new_column(0)), // Relative to the right input.
    //         }],
    //     };

    //     assert_eq!(expected, preprojections)
    // }

    // #[test]
    // fn input_preprojections_cast_left_and_right() {
    //     let on_expr = LogicalExpression::Binary {
    //         op: new_binary_eq(),
    //         left: Box::new(LogicalExpression::Cast {
    //             to: DataType::Float64,
    //             expr: Box::new(LogicalExpression::new_column(2)),
    //         }),
    //         right: Box::new(LogicalExpression::Cast {
    //             to: DataType::Float64,
    //             expr: Box::new(LogicalExpression::new_column(3)),
    //         }),
    //     };

    //     let preprojections = InputPreprojections::try_compute_from_on_expr(3, 2, on_expr).unwrap();

    //     let expected = InputPreprojections {
    //         on_expr: LogicalExpression::Binary {
    //             op: new_binary_eq(),
    //             left: Box::new(LogicalExpression::new_column(4)),
    //             right: Box::new(LogicalExpression::new_column(6)),
    //         },
    //         left_projections: vec![LogicalExpression::Cast {
    //             to: DataType::Float64,
    //             expr: Box::new(LogicalExpression::new_column(2)),
    //         }],
    //         right_projections: vec![LogicalExpression::Cast {
    //             to: DataType::Float64,
    //             expr: Box::new(LogicalExpression::new_column(0)),
    //         }],
    //     };

    //     assert_eq!(expected, preprojections)
    // }
}
