use crate::expr::{
    conjunction_expr::{ConjunctionExpr, ConjunctionOperator},
    Expression,
};

/// Recursively split an expression on AND, putting the split expressions in
/// `out`.
pub fn split_conjunction(expr: Expression, out: &mut Vec<Expression>) {
    match expr {
        Expression::Conjunction(ConjunctionExpr {
            expressions,
            op: ConjunctionOperator::And,
        }) => {
            for expr in expressions {
                split_conjunction(expr, out);
            }
        }
        other => out.push(other),
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
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(true),
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(false),
                }),
            ],
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
    fn split_conjunction_right_nested_and() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            expressions: vec![
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(true),
                }),
                Expression::Conjunction(ConjunctionExpr {
                    expressions: vec![
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Boolean(true),
                        }),
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Boolean(false),
                        }),
                    ],
                    op: ConjunctionOperator::And,
                }),
            ],
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

    #[test]
    fn split_conjunction_left_nested_and() {
        let expr = Expression::Conjunction(ConjunctionExpr {
            expressions: vec![
                Expression::Conjunction(ConjunctionExpr {
                    expressions: vec![
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Boolean(true),
                        }),
                        Expression::Literal(LiteralExpr {
                            literal: ScalarValue::Boolean(false),
                        }),
                    ],
                    op: ConjunctionOperator::And,
                }),
                Expression::Literal(LiteralExpr {
                    literal: ScalarValue::Boolean(true),
                }),
            ],

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
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(true),
            }),
        ];
        assert_eq!(expected, out);
    }
}
