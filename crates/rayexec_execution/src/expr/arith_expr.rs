use crate::{
    explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper},
    functions::scalar::{arith, ScalarFunction},
};
use std::fmt;

use super::{AsScalarFunction, Expression};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOperator {
    Add,
    Sub,
    Div,
    Mul,
    Mod,
}

impl AsScalarFunction for ArithOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::Add => &arith::Add,
            Self::Sub => &arith::Sub,
            Self::Div => &arith::Div,
            Self::Mul => &arith::Mul,
            Self::Mod => &arith::Rem,
        }
    }
}

impl fmt::Display for ArithOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Add => write!(f, "+"),
            Self::Sub => write!(f, "-"),
            Self::Div => write!(f, "/"),
            Self::Mul => write!(f, "*"),
            Self::Mod => write!(f, "%"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArithExpr {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub op: ArithOperator,
}

impl ContextDisplay for ArithExpr {
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
            ContextDisplayWrapper::with_mode(self.right.as_ref(), mode)
        )
    }
}
