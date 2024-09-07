use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};

use crate::expr::{literal_expr::LiteralExpr, Expression};

#[derive(Debug, Clone, PartialEq)]
pub enum FoldedExpression {
    /// An expression that underwent some amount of folding.
    Expression(Expression),

    /// An expression that was able to be folded into a constant.
    Constant(OwnedScalarValue),
}

impl FoldedExpression {
    pub fn try_unwrap_constant(self) -> Result<OwnedScalarValue> {
        match self {
            Self::Expression(expr) => Err(RayexecError::new(format!(
                "Expected a constant expression, got: {expr}"
            ))),
            FoldedExpression::Constant(c) => Ok(c),
        }
    }

    pub fn into_expr(self) -> Expression {
        match self {
            Self::Expression(expr) => expr,
            Self::Constant(constant) => Expression::Literal(LiteralExpr { literal: constant }),
        }
    }
}

/// Evaluate constant expressions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConstEval {}

impl ConstEval {
    pub fn fold(&self, expr: Expression) -> Result<FoldedExpression> {
        match expr {
            Expression::Literal(c) => Ok(FoldedExpression::Constant(c.literal)),
            other => {
                // TODO: Fancy folding goes here.
                Ok(FoldedExpression::Expression(other))
            }
        }
    }
}
