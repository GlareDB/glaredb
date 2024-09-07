use crate::{
    expr::{
        comparison_expr::ComparisonExpr,
        conjunction_expr::{ConjunctionExpr, ConjunctionOperator},
        Expression,
    },
    logical::{
        binder::bind_context::{BindContext, BindScopeRef},
        logical_join::{ComparisonCondition, JoinType},
    },
};
use rayexec_error::{not_implemented, RayexecError, Result};

#[derive(Debug, Default)]
pub struct ExtractedConditions {
    /// Join conditions successfully extracted from expressions.
    pub comparisons: Vec<ComparisonCondition>,
    /// Expressions that we could not build a condition for.
    ///
    /// These expressions should filter the output of a join.
    pub arbitrary: Vec<Expression>,
    /// Expressions that only rely on inputs on the left side.
    ///
    /// These should be placed into a filter prior to the join.
    pub left_filter: Vec<Expression>,
    /// Expressions that only rely on inputs on the right side.
    ///
    /// These should be placed into a filter prior to the join.
    pub right_filter: Vec<Expression>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprJoinSide {
    Left,
    Right,
    Both,
    None,
}

impl ExprJoinSide {
    fn combine(self, other: Self) -> ExprJoinSide {
        match (self, other) {
            (a, Self::None) => a,
            (Self::None, b) => b,
            (Self::Both, _) => Self::Both,
            (_, Self::Both) => Self::Both,
            (Self::Left, Self::Left) => Self::Left,
            (Self::Right, Self::Right) => Self::Right,
            _ => Self::Both,
        }
    }
}

#[derive(Debug)]
pub struct JoinConditionExtractor<'a> {
    pub bind_context: &'a BindContext,
    pub left_scope: BindScopeRef,
    pub right_scope: BindScopeRef,
    pub join_type: JoinType,
}

impl<'a> JoinConditionExtractor<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        left_scope: BindScopeRef,
        right_scope: BindScopeRef,
        join_type: JoinType,
    ) -> Self {
        JoinConditionExtractor {
            bind_context,
            left_scope,
            right_scope,
            join_type,
        }
    }

    /// Extracts expressions in join conditions, and pre-join filters.
    ///
    /// Extracted pre-join filters take into account the join type, and so
    /// should be able to be used in a LogicalFilter without additional checks.
    pub fn extract(&self, exprs: Vec<Expression>) -> Result<ExtractedConditions> {
        // Split on AND first.
        let mut split_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            split_conjunction(expr, &mut split_exprs);
        }

        let mut extracted = ExtractedConditions::default();

        for expr in split_exprs {
            let side = self.join_side(&expr)?;
            match side {
                ExprJoinSide::Both => {
                    // If we have a comparison expr, try to split it with each
                    // child expression referencing one side of the join.
                    match expr {
                        Expression::Comparison(ComparisonExpr { left, right, op }) => {
                            let left_side = self.join_side(&left)?;
                            let right_side = self.join_side(&right)?;

                            // If left and right expressions don't reference both
                            // sides, we can create a join condition for this.
                            if left_side != ExprJoinSide::Both && right_side != ExprJoinSide::Both {
                                debug_assert_ne!(left_side, right_side);

                                let mut condition = ComparisonCondition {
                                    left: *left,
                                    right: *right,
                                    op,
                                };

                                // If left expression is actually referencing right
                                // side, flip the condition.
                                if left_side == ExprJoinSide::Right {
                                    condition.flip_sides();
                                }

                                extracted.comparisons.push(condition);
                                continue;
                            }
                        }
                        other => {
                            extracted.arbitrary.push(other);
                        }
                    }
                }
                ExprJoinSide::Right => {
                    if self.join_type == JoinType::Left {
                        // Filter right input into LEFT join.
                        extracted.left_filter.push(expr);
                    } else {
                        extracted.arbitrary.push(expr);
                    }
                }
                ExprJoinSide::Left => {
                    if self.join_type == JoinType::Right {
                        // Filter left input into RIGHT join.
                        extracted.right_filter.push(expr);
                    } else {
                        extracted.arbitrary.push(expr);
                    }
                }
                _ => {
                    extracted.arbitrary.push(expr);
                }
            }
        }

        Ok(extracted)
    }

    /// Finds the side of a join an expression is referencing.
    fn join_side(&self, expr: &Expression) -> Result<ExprJoinSide> {
        self.join_side_inner(expr, ExprJoinSide::None)
    }

    fn join_side_inner(&self, expr: &Expression, side: ExprJoinSide) -> Result<ExprJoinSide> {
        match expr {
            Expression::Column(col) => {
                if self
                    .bind_context
                    .table_is_in_scope(self.left_scope, col.table_scope)?
                {
                    Ok(ExprJoinSide::Left)
                } else if self
                    .bind_context
                    .table_is_in_scope(self.right_scope, col.table_scope)?
                {
                    Ok(ExprJoinSide::Right)
                } else {
                    Err(RayexecError::new(format!(
                        "Cannot find join side for expression: {expr}"
                    )))
                }
            }
            Expression::Subquery(_) => not_implemented!("subquery in join condition"),
            other => {
                let mut side = side;
                other.for_each_child(&mut |expr| {
                    let new_side = self.join_side_inner(expr, side)?;
                    side = new_side.combine(side);
                    Ok(())
                })?;
                Ok(side)
            }
        }
    }
}

/// Recursively split an expression on AND, putting the split expressions in
/// `out`.
fn split_conjunction(expr: Expression, out: &mut Vec<Expression>) {
    fn inner(expr: Expression, out: &mut Vec<Expression>) -> Option<Expression> {
        if let Expression::Conjunction(ConjunctionExpr {
            left,
            right,
            op: ConjunctionOperator::And,
        }) = expr
        {
            out.push(*left);
            if let Some(other_expr) = inner(*right, out) {
                out.push(other_expr);
            }
            return None;
        }
        Some(expr)
    }

    if let Some(expr) = inner(expr, out) {
        out.push(expr)
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::ScalarValue;

    use crate::expr::literal_expr::LiteralExpr;

    use super::*;

    #[test]
    fn split_conjunction_none() {
        let expr = Expression::Literal(LiteralExpr {
            literal: ScalarValue::Int8(4),
        });

        let mut out = Vec::new();
        split_conjunction(expr.clone(), &mut out);

        let expected = vec![expr];
        assert_eq!(expected, out);
    }

    #[test]
    fn split_conjunction_single_and() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            left: Box::new(Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            })),
            right: Box::new(Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(false),
            })),
            op: ConjunctionOperator::And,
        });

        let mut out = Vec::new();
        split_conjunction(expr, &mut out);

        let expected = vec![
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(false),
            }),
        ];
        assert_eq!(expected, out);
    }

    #[test]
    fn split_conjunction_nested_and() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            left: Box::new(Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            })),
            right: Box::new(Expression::Conjunction(ConjunctionExpr {
                left: Box::new(Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(true),
                })),
                right: Box::new(Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(false),
                })),
                op: ConjunctionOperator::And,
            })),
            op: ConjunctionOperator::And,
        });

        let mut out = Vec::new();
        split_conjunction(expr, &mut out);

        let expected = vec![
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(false),
            }),
        ];
        assert_eq!(expected, out);
    }
}
