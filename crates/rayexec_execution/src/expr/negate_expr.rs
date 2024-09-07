use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use crate::{functions::scalar::ScalarFunction, logical::binder::bind_context::BindContext};

use super::{AsScalarFunction, Expression};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegateOperator {
    Not,    // Boolean,
    Negate, // Numeric
}

impl AsScalarFunction for NegateOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq)]
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

impl fmt::Display for NegateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.op {
            NegateOperator::Not => write!(f, "NOT({})", self.expr),
            NegateOperator::Negate => write!(f, "-{}", self.expr),
        }
    }
}
