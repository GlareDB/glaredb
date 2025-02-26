use std::fmt;

use super::{AsScalarFunctionSet, Expression};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::builtin::comparison::{
    FUNCTION_SET_EQ,
    FUNCTION_SET_GT,
    FUNCTION_SET_GT_EQ,
    FUNCTION_SET_LT,
    FUNCTION_SET_LT_EQ,
    FUNCTION_SET_NEQ,
};

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

impl AsScalarFunctionSet for ComparisonOperator {
    fn as_scalar_function_set(&self) -> &ScalarFunctionSet {
        match self {
            ComparisonOperator::Eq => &FUNCTION_SET_EQ,
            ComparisonOperator::NotEq => &FUNCTION_SET_NEQ,
            ComparisonOperator::Lt => &FUNCTION_SET_LT,
            ComparisonOperator::LtEq => &FUNCTION_SET_LT_EQ,
            ComparisonOperator::Gt => &FUNCTION_SET_GT,
            ComparisonOperator::GtEq => &FUNCTION_SET_GT_EQ,
            ComparisonOperator::IsDistinctFrom => unimplemented!(),
            ComparisonOperator::IsNotDistinctFrom => unimplemented!(),
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

impl ComparisonExpr {
    /// Flips the sides of the expression, including flipping the operatator.
    ///
    /// E.g. 'a >= b' becomes 'b <= a'
    pub fn flip_sides(&mut self) {
        self.op = self.op.flip();
        std::mem::swap(&mut self.left, &mut self.right);
    }
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
