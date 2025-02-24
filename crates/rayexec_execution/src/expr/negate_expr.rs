use std::fmt;

use rayexec_error::Result;

use super::{AsScalarFunctionSet, Expression};
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::ScalarFunction2;
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NegateOperator {
    Not,    // Boolean,
    Negate, // Numeric
}

impl AsScalarFunctionSet for NegateOperator {
    fn as_scalar_function_set(&self) -> &ScalarFunctionSet {
        unimplemented!()
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
