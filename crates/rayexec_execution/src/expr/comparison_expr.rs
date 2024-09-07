use crate::functions::scalar::{comparison, ScalarFunction};
use std::fmt;

use super::{AsScalarFunction, Expression};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl AsScalarFunction for ComparisonOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::Eq => &comparison::Eq,
            Self::NotEq => &comparison::Neq,
            Self::Lt => &comparison::Lt,
            Self::LtEq => &comparison::LtEq,
            Self::Gt => &comparison::Gt,
            Self::GtEq => &comparison::GtEq,
        }
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub op: ComparisonOperator,
}

impl fmt::Display for ComparisonExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
