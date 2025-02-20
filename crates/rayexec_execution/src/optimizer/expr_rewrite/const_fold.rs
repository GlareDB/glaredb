use rayexec_error::Result;

use super::ExpressionRewriteRule;
use crate::expr::literal_expr::LiteralExpr;
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;

/// Pre-compute constant expressions.
#[derive(Debug)]
pub struct ConstFold;

impl ExpressionRewriteRule for ConstFold {
    fn rewrite(table_list: &TableList, mut expression: Expression) -> Result<Expression> {
        maybe_fold(table_list, &mut expression)?;
        Ok(expression)
    }
}

fn maybe_fold(table_list: &TableList, expr: &mut Expression) -> Result<()> {
    if matches!(expr, Expression::Literal(_)) {
        return Ok(());
    }

    if expr.is_const_foldable() {
        let planner = PhysicalExpressionPlanner::new(table_list);
        let phys_expr = planner.plan_scalar(&[], expr)?;
        let mut evaluator = ExpressionEvaluator::try_new(vec![phys_expr], 1)?;
        let val = evaluator.try_eval_constant()?;

        // Our brand new expression.
        *expr = Expression::Literal(LiteralExpr { literal: val });

        return Ok(());
    }

    // Otherwise try the children.
    expr.for_each_child_mut(&mut |child| maybe_fold(table_list, child))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::expr::{add, and, cast, col_ref, lit};

    #[test]
    fn no_fold_literal() {
        let expr = lit("a");

        // No changes.
        let expected = lit("a");

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_string_to_float_cast() {
        let expr = cast(lit("3.1"), DataType::Float64);

        let expected = lit(3.1_f64);

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_true() {
        let expr = and([lit(true), lit(true)]).unwrap();

        let expected = lit(true);

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_false() {
        let expr = and([lit(true), lit(false)]).unwrap();

        let expected = lit(false);

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_add_numbers() {
        let expr = add(lit(4), lit(5));

        let expected = lit(9);

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_fold_col_ref() {
        let expr = add(col_ref(1, 1), lit(5));

        // No change
        let expected = add(col_ref(1, 1), lit(5));

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn partial_fold_col_ref() {
        let expr = add(col_ref(1, 1), add(lit(4), lit(5)));

        let expected = add(col_ref(1, 1), lit(9));

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }
}
