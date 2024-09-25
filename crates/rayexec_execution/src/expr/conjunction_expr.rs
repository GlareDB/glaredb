use crate::functions::scalar::{boolean, ScalarFunction};
use std::fmt;

use super::{AsScalarFunction, Expression};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConjunctionOperator {
    And,
    Or,
}

impl AsScalarFunction for ConjunctionOperator {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::And => &boolean::And,
            Self::Or => &boolean::Or,
        }
    }
}

impl fmt::Display for ConjunctionOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConjunctionExpr {
    pub op: ConjunctionOperator,
    pub expressions: Vec<Expression>,
}

impl fmt::Display for ConjunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.expressions.iter();

        write!(f, "(")?;
        match iter.next() {
            Some(expr) => write!(f, "{}", expr)?,
            None => return Ok(()),
        }

        for expr in iter {
            write!(f, " {} {}", self.op, expr)?;
        }
        write!(f, ")")?;

        Ok(())
    }
}
