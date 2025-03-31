use glaredb_error::Result;

use super::ExpressionRewriteRule;
use crate::expr::Expression;
use crate::expr::literal_expr::LiteralExpr;
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::table_list::TableList;

/// Pre-compute constant expressions.
#[derive(Debug)]
pub struct ConstFold;

impl ExpressionRewriteRule for ConstFold {
    fn rewrite(mut expression: Expression) -> Result<Expression> {
        maybe_fold(&mut expression)?;
        Ok(expression)
    }
}

fn maybe_fold(expr: &mut Expression) -> Result<()> {
    if matches!(expr, Expression::Literal(_)) {
        return Ok(());
    }

    if expr.is_const_foldable() {
        const EMPTY: &TableList = &TableList::empty();

        let planner = PhysicalExpressionPlanner::new(EMPTY);
        let phys_expr = planner.plan_scalar(&[], expr)?;
        let mut evaluator = ExpressionEvaluator::try_new(vec![phys_expr], 1)?;
        let val = evaluator.try_eval_constant()?;

        // Our brand new expression.
        *expr = Expression::Literal(LiteralExpr { literal: val });

        return Ok(());
    }

    // Return early for CASE to avoid attempting to evaluate the THEN side.
    //
    // TODO: Dont' do this, either
    // - Be selective about which expressions to fold (only the WHEN side, etc).
    // - Execute all expressions, allow errors, but don't report them for THEN exprs.
    if matches!(expr, Expression::Case(_)) {
        return Ok(());
    }

    // Otherwise try the children.
    expr.for_each_child_mut(&mut |child| maybe_fold(child))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::expr::{add, and, cast, column, lit};

    #[test]
    fn no_fold_literal() {
        let expr = lit("a").into();

        // No changes.
        let expected: Expression = lit("a").into();

        let got = ConstFold::rewrite(expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_string_to_float_cast() {
        let expr = cast(lit("3.1"), DataType::Float64).unwrap();

        let expected: Expression = lit(3.1_f64).into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_true() {
        let expr = and([lit(true).into(), lit(true).into()]).unwrap();

        let expected: Expression = lit(true).into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_false() {
        let expr = and([lit(true).into(), lit(false).into()]).unwrap();

        let expected: Expression = lit(false).into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_add_numbers() {
        let expr = add(lit(4), lit(5)).unwrap();

        let expected: Expression = lit(9).into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_fold_col_ref() {
        let expr = add(column((0, 1), DataType::Int32), lit(5)).unwrap();

        // No change
        let expected: Expression = add(column((0, 1), DataType::Int32), lit(5)).unwrap().into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn partial_fold_col_ref() {
        let expr = add(
            column((0, 1), DataType::Int32),
            add(lit(4), lit(5)).unwrap(),
        )
        .unwrap();

        // 4 + 5 => 9
        let expected: Expression = add(column((0, 1), DataType::Int32), lit(9)).unwrap().into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn partial_fold_col_ref_with_cast() {
        // Same as above but with the addition of an implicit cast from i32 to
        // i64 for the constant part since the non-constant part is i64. The
        // cast should be folded away leaving a literal value of the right type.

        let expr = add(
            column((0, 1), DataType::Int64),
            add(lit(4), lit(5)).unwrap(),
        )
        .unwrap();

        // 4 + 5 => 9
        let expected: Expression = add(column((0, 1), DataType::Int64), lit(9_i64))
            .unwrap()
            .into();

        let got = ConstFold::rewrite(expr.into()).unwrap();
        assert_eq!(expected, got);
    }
}
