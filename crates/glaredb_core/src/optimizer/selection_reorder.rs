use glaredb_error::Result;

use super::OptimizeRule;
use crate::expr::Expression;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::conjunction_expr::ConjunctionExpr;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::LogicalOperator;

/// Reorder predicate expressions in filters to try to have cheaper and more
/// selective expressions first in conjunction expressions.
///
/// The selection executor's short-circuiting logic can take advantage of this.
#[derive(Debug)]
pub struct SelectionReorder;

impl OptimizeRule for SelectionReorder {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        optimize_inner(&mut plan)?;

        for mat in bind_context.iter_materializations_mut() {
            optimize_inner(&mut mat.plan)?;
        }

        Ok(plan)
    }
}

fn optimize_inner(plan: &mut LogicalOperator) -> Result<()> {
    if let LogicalOperator::Filter(filter) = plan
        && let Expression::Conjunction(conj) = &mut filter.node.filter
    {
        // We have a conjunction, reorder!
        reorder_conj(conj);
    }

    for child in plan.children_mut() {
        optimize_inner(child)?;
    }

    Ok(())
}

fn reorder_conj(conj: &mut ConjunctionExpr) {
    /// Simple computation of expression cost. We assume that this is
    /// reordering for a boolean expression.
    ///
    /// Doesn't recurse into the expression.
    const fn expr_cost(expr: &Expression) -> u32 {
        match expr {
            Expression::Literal(_) => 0,
            Expression::Column(_) => 1, // Just getting a boolean column.
            Expression::Is(_) => 2,
            Expression::Comparison(cmp) => match cmp.op {
                ComparisonOperator::Eq | ComparisonOperator::IsNotDistinctFrom => 5,
                ComparisonOperator::NotEq | ComparisonOperator::IsDistinctFrom => 6,
                _ => 7,
            },
            _ => 50,
        }
    }

    conj.expressions.sort_unstable_by(|a, b| {
        let a_cost = expr_cost(a);
        let b_cost = expr_cost(b);
        a_cost.cmp(&b_cost)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::expr;
    use crate::functions::scalar::builtin::string::FUNCTION_SET_CONTAINS;

    #[test]
    fn reorder_conj_basic() {
        let mut expr = expr::and([
            expr::scalar_function(
                &FUNCTION_SET_CONTAINS,
                vec![
                    expr::column((0, 0), DataType::utf8()),
                    expr::lit("google").into(),
                ],
            )
            .unwrap()
            .into(),
            expr::eq(expr::column((0, 1), DataType::utf8()), expr::lit(""))
                .unwrap()
                .into(),
        ])
        .unwrap();

        reorder_conj(&mut expr);

        let expected = expr::and([
            expr::eq(expr::column((0, 1), DataType::utf8()), expr::lit(""))
                .unwrap()
                .into(),
            expr::scalar_function(
                &FUNCTION_SET_CONTAINS,
                vec![
                    expr::column((0, 0), DataType::utf8()),
                    expr::lit("google").into(),
                ],
            )
            .unwrap()
            .into(),
        ])
        .unwrap();

        assert_eq!(expected, expr);
    }
}
