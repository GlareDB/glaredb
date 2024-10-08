use std::collections::HashMap;

use rayexec_error::Result;

use super::ExpressionRewriteRule;
use crate::expr::conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, TableRef};

/// Rewrites join filter expressions containing ORs that reference both sides of
/// a join to an AND expression with the OR distributed.
///
/// Similar to the distributive or rewrite rule, but applies only to filters
/// that are part of a join condition.
///
/// '((#4.1 = 'FRANCE' AND #5.1 = 'GERMANY') OR (#4.1 = 'GERMANY' AND #5.1 = 'FRANCE'))'
/// =>
/// '((#4.1 = 'FRANCE' OR #4.1 = 'GERNANY') AND (#5.1 = 'FRANCE' OR #5.1 = 'GERMANY')) + <original expression>'
#[derive(Debug)]
pub struct JoinFilterOrRewrite;

impl ExpressionRewriteRule for JoinFilterOrRewrite {
    fn rewrite(_bind_context: &BindContext, mut expression: Expression) -> Result<Expression> {
        fn inner(expr: &mut Expression) -> Result<()> {
            match expr {
                Expression::Conjunction(conj) if conj.op == ConjunctionOperator::Or => {
                    maybe_rewrite_or(conj)?;
                    // Don't recurse here.
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

    // Check if any of the expression children are ANDs that reference multiple
    // tables.
    let can_rewrite = orig_expr.expressions.iter().any(|expr| match expr {
        Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            ..
        }) => expr.get_table_references().len() > 1,
        _ => false,
    });

    if !can_rewrite {
        return Ok(());
    }

    // Find all AND expressions in children, keyed by the table ref found the in
    // the expression.
    let mut extracted_and_exprs: Vec<_> = orig_expr
        .expressions
        .iter()
        .map(|expr| {
            let mut extracted = HashMap::new();
            extract_and_exprs(expr, &mut extracted);
            extracted
        })
        .collect();

    let first_extracted = match extracted_and_exprs.pop() {
        Some(extracted) => extracted,
        None => return Ok(()),
    };

    // Iterator through extracted exprs per child and check that a candidate
    // expression references a table found in all extracted child expressions.
    //
    // If valid, they get added to a new AND.
    let mut and_exprs = Vec::new();
    for (table, exprs) in first_extracted {
        let mut or_exprs = Vec::new();

        let mut valid = true;
        for other_extracted in &extracted_and_exprs {
            match other_extracted.get(&table) {
                Some(exprs) => {
                    or_exprs.extend(exprs.iter().cloned());
                }
                None => {
                    valid = false;
                    break;
                }
            }
        }

        if !valid {
            // Table reference not found in all child expressions.
            continue;
        }

        or_exprs.extend(exprs);

        and_exprs.push(Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::Or,
            expressions: or_exprs,
        }));
    }

    // Add the original OR expressions.
    and_exprs.push(Expression::Conjunction(orig_expr.clone()));

    // Generate new AND.
    *orig_expr = ConjunctionExpr {
        op: ConjunctionOperator::And,
        expressions: and_exprs,
    };

    Ok(())
}

fn extract_and_exprs(expr: &Expression, and_exprs: &mut HashMap<TableRef, Vec<Expression>>) {
    match expr {
        Expression::Conjunction(ConjunctionExpr {
            op: ConjunctionOperator::And,
            expressions,
        }) => {
            for child in expressions {
                extract_and_exprs(child, and_exprs);
            }
        }
        other => {
            let tables = other.get_table_references();
            if tables.len() != 1 {
                // Can only rewrite if this expression is referencing a single
                // table.
                return;
            }
            let table = tables.into_iter().next().expect("a table reference");
            and_exprs
                .entry(table)
                .and_modify(|exprs| exprs.push(other.clone()))
                .or_insert_with(|| vec![other.clone()]);
        }
    }
}
