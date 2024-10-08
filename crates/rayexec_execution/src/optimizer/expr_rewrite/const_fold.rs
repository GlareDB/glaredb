use crate::{
    expr::{literal_expr::LiteralExpr, physical::planner::PhysicalExpressionPlanner, Expression},
    logical::binder::bind_context::BindContext,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};

use super::ExpressionRewriteRule;

/// Pre-compute constant expressions.
#[derive(Debug)]
pub struct ConstFold;

impl ExpressionRewriteRule for ConstFold {
    fn rewrite(bind_context: &BindContext, mut expression: Expression) -> Result<Expression> {
        maybe_fold(bind_context, &mut expression)?;
        Ok(expression)
    }
}

fn maybe_fold(bind_context: &BindContext, expr: &mut Expression) -> Result<()> {
    if matches!(expr, Expression::Literal(_)) {
        return Ok(());
    }

    if expr.is_const_foldable() {
        let planner = PhysicalExpressionPlanner::new(bind_context);
        let phys_expr = planner.plan_scalar(&[], expr)?;
        let dummy = Batch::empty_with_num_rows(1);
        let val = phys_expr.eval(&dummy)?;

        if val.logical_len() != 1 {
            return Err(RayexecError::new(format!(
                "Expected 1 value from const eval, got {}",
                val.logical_len()
            )));
        }

        let val = val
            .logical_value(0) // Len checked above.
            .map_err(|_| {
                RayexecError::new(format!(
                    "Failed to get folded scalar value from expression: {expr}"
                ))
            })?;

        // Our brand new expression.
        *expr = Expression::Literal(LiteralExpr {
            literal: val.into_owned(),
        });

        return Ok(());
    }

    // Otherwise try the children.
    expr.for_each_child_mut(&mut |child| maybe_fold(bind_context, child))
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataType;

    use crate::expr::{add, and, cast, col_ref, lit};

    use super::*;

    #[test]
    fn no_fold_literal() {
        let expr = lit("a");

        // No changes.
        let expected = expr.clone();

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_string_to_float_cast() {
        let expr = cast(lit("3.1"), DataType::Float64);

        let expected = lit(3.1_f64);

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_true() {
        let expr = and([lit(true), lit(true)]).unwrap();

        let expected = lit(true);

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_and_true_false() {
        let expr = and([lit(true), lit(false)]).unwrap();

        let expected = lit(false);

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn fold_add_numbers() {
        let expr = add(lit(4), lit(5));

        let expected = lit(9);

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn no_fold_col_ref() {
        let expr = add(col_ref(1, 1), lit(5));

        // No change
        let expected = expr.clone();

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn partial_fold_col_ref() {
        let expr = add(col_ref(1, 1), add(lit(4), lit(5)));

        let expected = add(col_ref(1, 1), lit(9));

        let bind_context = BindContext::new();
        let got = ConstFold::rewrite(&bind_context, expr).unwrap();
        assert_eq!(expected, got);
    }
}
