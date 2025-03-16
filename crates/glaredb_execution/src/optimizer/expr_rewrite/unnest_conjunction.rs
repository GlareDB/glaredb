use glaredb_error::Result;

use super::ExpressionRewriteRule;
use crate::expr::conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use crate::expr::Expression;

/// Unnest nested AND or OR expressions.
///
/// 'a AND (b AND c) => a AND b AND c'
#[derive(Debug)]
pub struct UnnestConjunctionRewrite;

impl ExpressionRewriteRule for UnnestConjunctionRewrite {
    fn rewrite(mut expression: Expression) -> Result<Expression> {
        fn inner(expression: &mut Expression) {
            match expression {
                Expression::Conjunction(ConjunctionExpr { op, expressions }) => {
                    let mut new_expressions = Vec::with_capacity(expressions.len());
                    for expr in expressions.drain(..) {
                        unnest_op(expr, *op, &mut new_expressions);
                    }

                    *expression = Expression::Conjunction(ConjunctionExpr {
                        op: *op,
                        expressions: new_expressions,
                    });

                    // Recurse into the children too.
                    expression
                        .for_each_child_mut(&mut |child| {
                            inner(child);
                            Ok(())
                        })
                        .expect("unnest to not fail")
                }
                other => other
                    .for_each_child_mut(&mut |child| {
                        inner(child);
                        Ok(())
                    })
                    .expect("unnest to not fail"),
            }
        }

        inner(&mut expression);

        Ok(expression)
    }
}

fn unnest_op(expr: Expression, search_op: ConjunctionOperator, out: &mut Vec<Expression>) {
    match expr {
        Expression::Conjunction(ConjunctionExpr { op, expressions }) if op == search_op => {
            for expr in expressions {
                unnest_op(expr, search_op, out);
            }
        }
        other => out.push(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::expr::{and, column, or};

    #[test]
    fn unnest_none() {
        // '(c0 AND c1)' => '(c0 AND c1)'
        let c0: Expression = column((0, 0), DataType::Boolean).into();
        let c1: Expression = column((0, 1), DataType::Boolean).into();

        let expr: Expression = and([c0, c1]).unwrap().into();

        // No change.
        let expected = expr.clone();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn unnest_one_level() {
        // '(c0 AND (c1 AND c2))' => '(c0 AND c1 AND c2)'
        let c0: Expression = column((0, 0), DataType::Boolean).into();
        let c1: Expression = column((0, 1), DataType::Boolean).into();
        let c2: Expression = column((0, 2), DataType::Boolean).into();

        let expr: Expression = and([c0.clone(), and([c1.clone(), c2.clone()]).unwrap().into()])
            .unwrap()
            .into();

        let expected: Expression = and([c0, c1, c2]).unwrap().into();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_unnest_different_ops() {
        // '(c0 AND (c1 OR c2))' => '(c0 AND (c1 OR c2))'
        let c0: Expression = column((0, 0), DataType::Boolean).into();
        let c1: Expression = column((0, 1), DataType::Boolean).into();
        let c2: Expression = column((0, 2), DataType::Boolean).into();

        let expr: Expression = and([c0, or([c1, c2]).unwrap().into()]).unwrap().into();

        // No change.
        let expected = expr.clone();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_unnest_different_ops_nested() {
        // '(c0 AND (c1 OR (c2 AND c3)))' => '(c0 AND (c1 OR (c2 AND c3)))'
        let c0: Expression = column((0, 0), DataType::Boolean).into();
        let c1: Expression = column((0, 1), DataType::Boolean).into();
        let c2: Expression = column((0, 2), DataType::Boolean).into();
        let c3: Expression = column((0, 3), DataType::Boolean).into();

        let expr: Expression = and([c0, or([c1, and([c2, c3]).unwrap().into()]).unwrap().into()])
            .unwrap()
            .into();

        // No change.
        let expected = expr.clone();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn unnest_different_ops_nested() {
        // '(c0 AND (c1 OR (c2 AND (c3 AND c4))))' => '(c0 AND (c1 OR (c2 AND c3 AND c4)))'
        let c0: Expression = column((0, 0), DataType::Boolean).into();
        let c1: Expression = column((0, 1), DataType::Boolean).into();
        let c2: Expression = column((0, 2), DataType::Boolean).into();
        let c3: Expression = column((0, 3), DataType::Boolean).into();
        let c4: Expression = column((0, 4), DataType::Boolean).into();

        let expr: Expression = and([
            c0.clone(),
            or([
                c1.clone(),
                and([c2.clone(), and([c3.clone(), c4.clone()]).unwrap().into()])
                    .unwrap()
                    .into(),
            ])
            .unwrap()
            .into(),
        ])
        .unwrap()
        .into();

        let expected: Expression = and([
            c0,
            or([c1, and([c2, c3, c4]).unwrap().into()]).unwrap().into(),
        ])
        .unwrap()
        .into();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn unnest_three_levels() {
        // '(0 AND (1 AND (2 AND 3)))' => '(0 AND 1 AND 2 AND 3)'
        let c0: Expression = column((0, 0), DataType::Boolean).into();
        let c1: Expression = column((0, 1), DataType::Boolean).into();
        let c2: Expression = column((0, 2), DataType::Boolean).into();
        let c3: Expression = column((0, 3), DataType::Boolean).into();

        let expr: Expression = and([
            c0.clone(),
            and([c1.clone(), and([c2.clone(), c3.clone()]).unwrap().into()])
                .unwrap()
                .into(),
        ])
        .unwrap()
        .into();

        let expected: Expression = and([c0, c1, c2, c3]).unwrap().into();

        let got = UnnestConjunctionRewrite::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }
}
