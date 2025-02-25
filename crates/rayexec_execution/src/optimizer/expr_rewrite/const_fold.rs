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
        let expr = lit("a").into();

        // No changes.
        let expected: Expression = lit("a").into();

        let table_list = TableList::empty();
        let got = ConstFold::rewrite(&table_list, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_string_to_float_cast() {
        let table_list = TableList::empty();
        let expr = cast(lit("3.1").into(), DataType::Float64);

        let expected: Expression = lit(3.1_f64).into();

        let got = ConstFold::rewrite(&table_list, expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_true() {
        let table_list = TableList::empty();
        let expr = and(&table_list, [lit(true).into(), lit(true).into()]).unwrap();

        let expected: Expression = lit(true).into();

        let got = ConstFold::rewrite(&table_list, expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_false() {
        let table_list = TableList::empty();
        let expr = and(&table_list, [lit(true).into(), lit(false).into()]).unwrap();

        let expected: Expression = lit(false).into();

        let got = ConstFold::rewrite(&table_list, expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_add_numbers() {
        let table_list = TableList::empty();
        let expr = add(&table_list, lit(4), lit(5)).unwrap();

        let expected: Expression = lit(9).into();

        let got = ConstFold::rewrite(&table_list, expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_fold_col_ref() {
        let mut table_list = TableList::empty();
        let t0 = table_list
            .push_table(None, [DataType::Int8, DataType::Utf8], ["c1", "c2"])
            .unwrap();

        let expr = add(&table_list, col_ref(t0, 1), lit(5)).unwrap();

        // No change
        let expected: Expression = add(&table_list, col_ref(t0, 1), lit(5)).unwrap().into();

        let got = ConstFold::rewrite(&table_list, expr.into()).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn partial_fold_col_ref() {
        let mut table_list = TableList::empty();
        let t0 = table_list
            .push_table(None, [DataType::Int8, DataType::Utf8], ["c1", "c2"])
            .unwrap();

        let expr = add(
            &table_list,
            col_ref(t0, 1),
            add(&table_list, lit(4), lit(5)).unwrap(),
        )
        .unwrap();

        let expected: Expression = add(&table_list, col_ref(t0, 1), lit(9)).unwrap().into();

        let got = ConstFold::rewrite(&table_list, expr.into()).unwrap();
        assert_eq!(expected, got);
    }
}
