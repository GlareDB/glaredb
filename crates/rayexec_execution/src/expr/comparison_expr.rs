use std::fmt;

use super::{AsScalarFunction, Expression};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::scalar::builtin::comparison;
use crate::functions::scalar::ScalarFunction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

impl ComparisonOperator {
    pub fn flip(self) -> Self {
        match self {
            ComparisonOperator::Eq => ComparisonOperator::Eq,
            ComparisonOperator::NotEq => ComparisonOperator::NotEq,
            ComparisonOperator::Lt => ComparisonOperator::Gt,
            ComparisonOperator::LtEq => ComparisonOperator::GtEq,
            ComparisonOperator::Gt => ComparisonOperator::Lt,
            ComparisonOperator::GtEq => ComparisonOperator::LtEq,
            ComparisonOperator::IsDistinctFrom => ComparisonOperator::IsDistinctFrom,
            ComparisonOperator::IsNotDistinctFrom => ComparisonOperator::IsDistinctFrom,
        }
    }

    pub fn negate(self) -> Self {
        match self {
            ComparisonOperator::Eq => ComparisonOperator::NotEq,
            ComparisonOperator::NotEq => ComparisonOperator::Eq,
            ComparisonOperator::Lt => ComparisonOperator::GtEq,
            ComparisonOperator::LtEq => ComparisonOperator::Gt,
            ComparisonOperator::Gt => ComparisonOperator::LtEq,
            ComparisonOperator::GtEq => ComparisonOperator::Lt,
            ComparisonOperator::IsDistinctFrom => ComparisonOperator::IsNotDistinctFrom,
            ComparisonOperator::IsNotDistinctFrom => ComparisonOperator::IsDistinctFrom,
        }
    }
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
            Self::IsDistinctFrom => &comparison::IsDistinctFrom,
            Self::IsNotDistinctFrom => &comparison::IsNotDistinctFrom,
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
            Self::IsDistinctFrom => write!(f, "IS DISTINCT FROM"),
            Self::IsNotDistinctFrom => write!(f, "IS NOT DISTINCT FROM"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComparisonExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub op: ComparisonOperator,
}

impl ContextDisplay for ComparisonExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            ContextDisplayWrapper::with_mode(self.left.as_ref(), mode),
            self.op,
            ContextDisplayWrapper::with_mode(self.right.as_ref(), mode),
        )
    }
}
