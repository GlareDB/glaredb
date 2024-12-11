use indexmap::IndexSet;
use rayexec_error::{RayexecError, Result};

use super::ExpressionRewriteRule;
use crate::expr::conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableList;

/// Tries to lift up AND expressions through OR expressions
///
/// '(a AND b) OR (a AND c) OR (a AND d) = a AND (b OR c OR d)'
#[derive(Debug)]
pub struct DistributiveOrRewrite;

impl ExpressionRewriteRule for DistributiveOrRewrite {
    fn rewrite(_table_list: &TableList, mut expression: Expression) -> Result<Expression> {
        fn inner(expr: &mut Expression) -> Result<()> {
            match expr {
                Expression::Conjunction(conj) if conj.op == ConjunctionOperator::Or => {
                    maybe_rewrite_or(conj)?;

                    // Go down through children too.
                    for child in &mut conj.expressions {
                        inner(child)?;
                    }

                    Ok(())
                }
                other => other.for_each_child_mut(&mut inner),
            }
        }

        inner(&mut expression)?;

        Ok(expression)
    }
}

fn maybe_rewrite_or(orig_expr: &mut ConjunctionExpr) -> Result<()> {
    assert_eq!(ConjunctionOperator::Or, orig_expr.op);

    let mut child_iter = orig_expr.expressions.iter();

    // Initialize common expression with the expressions found in the first
    // child.
    let mut common_exprs = IndexSet::new(); // Using index set just for test ordering.
    match child_iter.next() {
        Some(child) => {
            insert_children_to_common_set(child, &mut common_exprs);
        }
        None => return Err(RayexecError::new("Missing child expression for OR")),
    }

    // For each additional child, find its candidates, then intersect with
    // existing candidates.
    for child in child_iter {
        let mut candidates = IndexSet::new();
        insert_children_to_common_set(child, &mut candidates);

        common_exprs.retain(|expr| candidates.contains(expr));
    }

    if common_exprs.is_empty() {
        // No common exprs to extract.
        return Ok(());
    }

    // Expressions that will be included in the top-level AND.
    let common_exprs: IndexSet<_> = common_exprs.into_iter().cloned().collect();

    let mut new_or_children = Vec::with_capacity(orig_expr.expressions.len());

    // Update original child expressions in the OR to no longer contain the
    // common expressions.
    for or_expr_child in orig_expr.expressions.drain(..) {
        match or_expr_child {
            Expression::Conjunction(ConjunctionExpr {
                op: ConjunctionOperator::And,
                expressions,
            }) => {
                // Remove any children that will be in the top-level AND.
                let mut new_and_children: Vec<_> = expressions
                    .into_iter()
                    .filter(|expr| !common_exprs.contains(expr))
                    .collect();

                match new_and_children.len() {
                    0 => {
                        // All AND expressions were pulled out.
                    }
                    1 => {
                        // We have single AND child remaining, just use that
                        // instead.
                        new_or_children.push(new_and_children.pop().unwrap());
                    }
                    _ => {
                        // Add the modified AND to the OR
                        new_or_children.push(Expression::Conjunction(ConjunctionExpr {
                            op: ConjunctionOperator::And,
                            expressions: new_and_children,
                        }));
                    }
                }
            }
            other => {
                if !common_exprs.contains(&other) {
                    new_or_children.push(other);
                }
            }
        }
    }

    // OR expression now becomes an AND expression.
    *orig_expr = ConjunctionExpr {
        op: ConjunctionOperator::And,
        // AND all common expressions along with an OR containing expressions we
        // weren't able to pull up.
        expressions: common_exprs.into_iter().collect(),
    };

    match new_or_children.len() {
        0 => {
            // Everything was eliminated from the OR expression.
            //
            // Would happen in a case like '(a AND b) OR (a AND b)' which is
            // just the same as '(a AND b)'
        }
        1 => {
            // We have a single remaining child in the OR.
            //
            // This isn't useful on its own, so just append directly to the AND
            // expression.
            //
            // Would happen in a case like 'a OR (a AND b)'.
            orig_expr.expressions.append(&mut new_or_children);
        }
        _ => {
            // Multiple OR children remain. Include an appropriate OR
            // expression.
            orig_expr
                .expressions
                .push(Expression::Conjunction(ConjunctionExpr {
                    op: ConjunctionOperator::Or,
                    expressions: new_or_children,
                }));
        }
    }

    Ok(())
}

/// If this child expression is an AND expression, insert its children into the
/// set. Otherwise just insert the expression itself.
fn insert_children_to_common_set<'a>(child: &'a Expression, exprs: &mut IndexSet<&'a Expression>) {
    match child {
        Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions,
        }) => {
            exprs.extend(expressions);
        }
        other => {
            exprs.insert(other);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{and, lit, or};

    #[test]
    fn distribute_none() {
        // '(0 AND 1) OR (2 AND 3)' => '(0 AND 1)'
        let expr = or([
            and([lit(0), lit(1)]).unwrap(),
            and([lit(2), lit(3)]).unwrap(),
        ])
        .unwrap();

        // No changes.
        let expected = expr.clone();

        let table_list = TableList::empty();
        let got = DistributiveOrRewrite::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn distribute_eliminate_redundant_or() {
        // '(0 AND 1) OR (0 AND 1)' => '(0 AND 1)'
        let expr = or([
            and([lit(0), lit(1)]).unwrap(),
            and([lit(0), lit(1)]).unwrap(),
        ])
        .unwrap();

        let expected = and([lit(0), lit(1)]).unwrap();

        let table_list = TableList::empty();
        let got = DistributiveOrRewrite::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn distribute_eliminate_or_with_single_remaining() {
        // '(0) OR (0 AND 1)' => '(0 AND 1)'
        let expr = or([lit(0), and([lit(0), lit(1)]).unwrap()]).unwrap();

        let expected = and([lit(0), lit(1)]).unwrap();

        let table_list = TableList::empty();
        let got = DistributiveOrRewrite::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn distribute_or_keep_inner_and() {
        // '(0 AND 1 AND 2 AND 3) OR (0 AND 4)
        // =>
        // '((0) AND ((1 AND 2 AND 3) OR (4))'
        let expr = or([
            and([lit(0), lit(1), lit(2), lit(3)]).unwrap(),
            and([lit(0), lit(4)]).unwrap(),
        ])
        .unwrap();

        let expected = and([
            lit(0),
            or([and([lit(1), lit(2), lit(3)]).unwrap(), lit(4)]).unwrap(),
        ])
        .unwrap();

        let table_list = TableList::empty();
        let got = DistributiveOrRewrite::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got, "expected: {expected:#?}\n, got: {got:#?}");
    }
}
