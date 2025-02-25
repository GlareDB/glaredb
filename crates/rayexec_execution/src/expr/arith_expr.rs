use std::fmt;

use rayexec_error::Result;

use super::{AsScalarFunctionSet, Expression};
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::builtin::arith::{
    FUNCTION_SET_ADD,
    FUNCTION_SET_DIV,
    FUNCTION_SET_MUL,
    FUNCTION_SET_REM,
    FUNCTION_SET_SUB,
};
use crate::functions::scalar::PlannedScalarFunction;
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOperator {
    Add,
    Sub,
    Div,
    Mul,
    Mod,
}

impl AsScalarFunctionSet for ArithOperator {
    fn as_scalar_function_set(&self) -> &ScalarFunctionSet {
        match self {
            Self::Add => &FUNCTION_SET_ADD,
            Self::Sub => &FUNCTION_SET_SUB,
            Self::Div => &FUNCTION_SET_DIV,
            Self::Mul => &FUNCTION_SET_MUL,
            Self::Mod => &FUNCTION_SET_REM,
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
    pub op: ArithOperator,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub return_type: DataType,
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
