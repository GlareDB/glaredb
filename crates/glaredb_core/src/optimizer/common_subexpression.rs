use std::collections::HashMap;

use glaredb_error::Result;

use super::OptimizeRule;
use crate::expr::Expression;
use crate::expr::column_expr::ColumnExpr;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LocationRequirement, LogicalNode, LogicalOperator, Node};

#[derive(Debug, Clone, Copy)]
pub struct CommonSubExpression;

impl OptimizeRule for CommonSubExpression {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Optimize base plan.
        optimizer_inner(&mut plan, bind_context)?;

        // TODO: Come up with a good pattern that satisfies lifetimes.
        // // Optimize materializations.
        // for mat in bind_context.iter_materializations_mut() {
        //     optimizer_inner(&mut mat.plan, bind_context)?;
        // }

        Ok(plan)
    }
}

fn optimizer_inner(operator: &mut LogicalOperator, bind_context: &mut BindContext) -> Result<()> {
    match operator {
        LogicalOperator::Project(n) => optimize_operator(n, bind_context),
        LogicalOperator::Aggregate(n) => optimize_operator(n, bind_context),
        _ => {
            // Everything else, just skip and go to children.
            for child in operator.children_mut() {
                optimizer_inner(child, bind_context)?;
            }
            Ok(())
        }
    }
}

fn optimize_operator<N>(operator: &mut Node<N>, bind_context: &mut BindContext) -> Result<()>
where
    Node<N>: LogicalNode,
{
    debug_assert_eq!(1, operator.children.len());

    let mut extracted = HashMap::new();
    // Extract expressions.
    operator.for_each_expr(|expr| extract_expressions(expr, &mut extracted))?;

    let has_common_subexprs = extracted.values().any(|&count| count > 1);
    if !has_common_subexprs {
        // Just walk this operator's child.
        return optimizer_inner(&mut operator.children[0], bind_context);
    }

    // We have common subexpressions, create a new projection
    // containing those expression

    let table_ref = bind_context.new_ephemeral_table()?;

    // Set of common expressions eligble to replace.
    let mut cse_replacements: HashMap<Expression, Option<ColumnExpr>> = extracted
        .into_iter()
        .filter_map(|(expr, count)| {
            if count > 1 {
                Some((expr.clone(), None))
            } else {
                None
            }
        })
        .collect();

    // New projections, udpated as we replace.
    let mut projections = Vec::new();

    operator.for_each_expr_mut(|expr| {
        replace_expressions(
            expr,
            table_ref,
            bind_context,
            &mut cse_replacements,
            &mut projections,
        )
    })?;

    // Pop the old child, we'll be inserting a new projection between the child
    // and the current operator.
    let mut child = operator.children.pop().expect("a single child");
    // Optimize child before putting it on the project.
    optimizer_inner(&mut child, bind_context)?;

    // Update this operator's children with a new projection.
    operator.children.push(LogicalOperator::Project(Node {
        node: LogicalProject {
            projections,
            projection_table: table_ref,
        },
        location: LocationRequirement::Any,
        estimated_cardinality: child.estimated_cardinality(),
        children: vec![child],
    }));

    Ok(())
}

/// Replace common expressions with a column expression referencing the new
/// projection.
fn replace_expressions(
    expr: &mut Expression,
    proj_ref: TableRef,
    bind_context: &mut BindContext,
    cse_replacements: &mut HashMap<Expression, Option<ColumnExpr>>,
    projections: &mut Vec<Expression>,
) -> Result<()> {
    match cse_replacements.get_mut(expr) {
        Some(col_expr) => {
            // This is a common expression. Check if it's already part of the
            // projectons.
            match col_expr.as_mut() {
                Some(col_expr) => {
                    // Expression already in projections, just update the
                    // current expression to the column.
                    *expr = Expression::from(col_expr.clone());
                    Ok(())
                }
                None => {
                    // Expression not in projection. Create a new column ref and
                    // add it to projections.
                    let col_idx = bind_context.push_column_for_table(
                        proj_ref,
                        "__generated_cse_col_ref",
                        expr.datatype()?,
                    )?;
                    let new_col_expr = ColumnExpr {
                        reference: (proj_ref, col_idx).into(),
                        datatype: expr.datatype()?,
                    };
                    // Ensure we share the same column ref for other expression.
                    *col_expr = Some(new_col_expr.clone());
                    let orig = std::mem::replace(expr, Expression::from(new_col_expr));
                    projections.push(orig);
                    Ok(())
                }
            }
        }
        None => {
            // Not a common expression, check children.
            expr.for_each_child_mut(|child| {
                replace_expressions(child, proj_ref, bind_context, cse_replacements, projections)
            })
        }
    }
}

/// Extracts non-trivial, sub-expressions (potentially including itself) into
/// the hashmap.
fn extract_expressions<'a>(
    expr: &'a Expression,
    extracted: &mut HashMap<&'a Expression, usize>,
) -> Result<()> {
    if expr.is_volatile() {
        // Nothing we should try to do for this expression.
        return Ok(());
    }

    match expr {
        Expression::Column(_) | Expression::Literal(_) => return Ok(()), // Trivial.
        Expression::Aggregate(_) | Expression::Window(_) => {
            // Don't try to move the aggregate itself, just handle its children.
            expr.for_each_child(|child| extract_expressions(child, extracted))
        }
        Expression::Case(_) => return Ok(()), // Short circuit eval, just avoid for now.
        Expression::Conjunction(_) => return Ok(()), // Short circuit eval, just avoid for now.
        Expression::Subquery(_) => return Ok(()), // Shouldn't be reachable by this point.
        Expression::Arith(_)
        | Expression::Between(_)
        | Expression::Cast(_)
        | Expression::Comparison(_)
        | Expression::Is(_)
        | Expression::Negate(_)
        | Expression::Unnest(_)
        | Expression::GroupingSet(_)
        | Expression::ScalarFunction(_) => {
            // "Normal" expressions we can extract.
            *extracted.entry(expr).or_insert(0) += 1;

            // Walk children.
            expr.for_each_child(|child| extract_expressions(child, extracted))
        }
    }
}
