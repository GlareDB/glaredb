use crate::functions::scalar::{boolean, ScalarFunction};
use std::fmt;

use super::{AsScalarFunction, Expression};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConjunctionOperator {
    And,
    Or,
}

impl AsScalarFunction for ConjunctionOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::And => &boolean::And,
            Self::Or => &boolean::Or,
        }
    }
}

impl fmt::Display for ConjunctionOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConjunctionExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub op: ConjunctionOperator,
}

impl fmt::Display for ConjunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
