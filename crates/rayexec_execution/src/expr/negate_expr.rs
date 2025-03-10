use std::fmt;

use rayexec_error::Result;

use super::{AsScalarFunctionSet, Expression};
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::builtin::negate::{FUNCTION_SET_NEGATE, FUNCTION_SET_NOT};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NegateOperator {
    Not,    // Boolean,
    Negate, // Numeric
}

impl AsScalarFunctionSet for NegateOperator {
    fn as_scalar_function_set(&self) -> &ScalarFunctionSet {
        match self {
            Self::Not => &FUNCTION_SET_NOT,
            Self::Negate => &FUNCTION_SET_NEGATE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NegateExpr {
    pub op: NegateOperator,
    pub expr: Box<Expression>,
}

impl NegateExpr {
    pub fn datatype(&self) -> Result<DataType> {
        Ok(match self.op {
            NegateOperator::Not => DataType::Boolean,
            NegateOperator::Negate => self.expr.datatype()?,
        })
    }
}

impl ContextDisplay for NegateExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self.op {
            NegateOperator::Not => write!(
                f,
                "NOT({})",
                ContextDisplayWrapper::with_mode(self.expr.as_ref(), mode),
            ),
            NegateOperator::Negate => write!(
                f,
                "-{}",
                ContextDisplayWrapper::with_mode(self.expr.as_ref(), mode),
            ),
        }
    }
}
