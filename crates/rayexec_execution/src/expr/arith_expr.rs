use std::fmt;

use rayexec_error::Result;

use super::{AsScalarFunction, Expression};
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::scalar::builtin::arith;
use crate::functions::scalar::ScalarFunction2;
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOperator {
    Add,
    Sub,
    Div,
    Mul,
    Mod,
}

impl AsScalarFunction for ArithOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction2 {
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

impl ArithExpr {
    pub fn datatype(&self, table_list: &TableList) -> Result<DataType> {
        // TODO: Be more efficient here.
        Ok(self
            .op
            .as_scalar_function()
            .plan(
                table_list,
                vec![self.left.as_ref().clone(), self.right.as_ref().clone()],
            )?
            .return_type)
    }
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
