use std::fmt;

use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use super::{AsScalarFunction, Expression};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::scalar::builtin::negate;
use crate::functions::scalar::ScalarFunction;
use crate::logical::binder::table_list::TableList;

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
    pub fn datatype(&self, table_list: &TableList) -> Result<DataType> {
        Ok(match self.op {
            NegateOperator::Not => DataType::Boolean,
            NegateOperator::Negate => {
                // Sure
                self.op
                    .as_scalar_function()
                    .plan(table_list, vec![self.expr.as_ref().clone()])?
                    .return_type
            }
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
