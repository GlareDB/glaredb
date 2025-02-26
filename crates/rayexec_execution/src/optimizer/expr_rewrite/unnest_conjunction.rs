use rayexec_error::Result;

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
    use crate::expr::{and, lit, or};

    // #[test]
    // fn unnest_none() {
    //     // '(0 AND 1)' => '(0 AND 1)'
    //     let expr = and([lit(0), lit(1)]).unwrap();

    //     // No change.
    //     let expected = expr.clone();

    //     let table_list = TableList::empty();
    //     let got = UnnestConjunctionRewrite::rewrite(&table_list, expr).unwrap();
    //     assert_eq!(expected, got);
    // }

    // #[test]
    // fn unnest_one_level() {
    //     // '(0 AND (1 AND 2))' => '(0 AND 1 AND 2)'
    //     let expr = and([lit(0), and([lit(1), lit(2)]).unwrap()]).unwrap();

    //     let expected = and([lit(0), lit(1), lit(2)]).unwrap();

    //     let table_list = TableList::empty();
    //     let got = UnnestConjunctionRewrite::rewrite(&table_list, expr).unwrap();
    //     assert_eq!(expected, got);
    // }

    // #[test]
    // fn no_unnest_different_ops() {
    //     // '(0 AND (1 OR 2))' => '(0 AND (1 OR 2))'
    //     let expr = and([lit(0), or([lit(1), lit(2)]).unwrap()]).unwrap();

    //     // No change.
    //     let expected = expr.clone();

    //     let table_list = TableList::empty();
    //     let got = UnnestConjunctionRewrite::rewrite(&table_list, expr).unwrap();
    //     assert_eq!(expected, got);
    // }

    // #[test]
    // fn no_unnest_different_ops_nested() {
    //     // '(0 AND (1 OR (2 AND 3)))' => '(0 AND (1 OR (2 AND 3)))'
    //     let expr = and([
    //         lit(0),
    //         or([lit(1), and([lit(2), lit(3)]).unwrap()]).unwrap(),
    //     ])
    //     .unwrap();

    //     // No change.
    //     let expected = expr.clone();

    //     let table_list = TableList::empty();
    //     let got = UnnestConjunctionRewrite::rewrite(&table_list, expr).unwrap();
    //     assert_eq!(expected, got);
    // }

    // #[test]
    // fn unnest_different_ops_nested() {
    //     // '(0 AND (1 OR (2 AND (3 AND 4))))' => '(0 AND (1 OR (2 AND 3 AND 4)))'
    //     let expr = and([
    //         lit(0),
    //         or([
    //             lit(1),
    //             and([lit(2), and([lit(3), lit(4)]).unwrap()]).unwrap(),
    //         ])
    //         .unwrap(),
    //     ])
    //     .unwrap();

    //     let expected = and([
    //         lit(0),
    //         or([lit(1), and([lit(2), lit(3), lit(4)]).unwrap()]).unwrap(),
    //     ])
    //     .unwrap();

    //     let table_list = TableList::empty();
    //     let got = UnnestConjunctionRewrite::rewrite(&table_list, expr).unwrap();
    //     assert_eq!(expected, got);
    // }

    // #[test]
    // fn unnest_three_levels() {
    //     // '(0 AND (1 AND (2 AND 3)))' => '(0 AND 1 AND 2 AND 3)'
    //     let expr = and([
    //         lit(0),
    //         and([lit(1), and([lit(2), lit(3)]).unwrap()]).unwrap(),
    //     ])
    //     .unwrap();

    //     let expected = and([lit(0), lit(1), lit(2), lit(3)]).unwrap();

    //     let table_list = TableList::empty();
    //     let got = UnnestConjunctionRewrite::rewrite(&table_list, expr).unwrap();
    //     assert_eq!(expected, got);
    // }
}
