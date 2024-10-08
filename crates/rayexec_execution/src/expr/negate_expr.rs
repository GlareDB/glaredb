use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use crate::{
    explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper},
    functions::scalar::{negate, ScalarFunction},
    logical::binder::bind_context::BindContext,
};

use super::{AsScalarFunction, Expression};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NegateOperator {
    Not,    // Boolean,
    Negate, // Numeric
}

impl AsScalarFunction for NegateOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::Not => &negate::Not,
            Self::Negate => &negate::Negate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NegateExpr {
    pub op: NegateOperator,
    pub expr: Box<Expression>,
}

impl NegateExpr {
    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        Ok(match self.op {
            NegateOperator::Not => DataType::Boolean,
            NegateOperator::Negate => self
                .op
                .as_scalar_function()
                .plan_from_expressions(bind_context, &[&self.expr])?
                .return_type(),
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
