use std::fmt;

use super::{AsScalarFunctionSet, Expression};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::function_set::ScalarFunctionSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConjunctionOperator {
    And,
    Or,
}

impl AsScalarFunctionSet for ConjunctionOperator {
    fn as_scalar_function_set(&self) -> &ScalarFunctionSet {
        unimplemented!()
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

impl ContextDisplay for ConjunctionExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let mut iter = self.expressions.iter();

        write!(f, "(")?;
        match iter.next() {
            Some(expr) => write!(f, "{}", ContextDisplayWrapper::with_mode(expr, mode),)?,
            None => return Ok(()),
        }

        for expr in iter {
            write!(
                f,
                " {} {}",
                self.op,
                ContextDisplayWrapper::with_mode(expr, mode),
            )?;
        }
        write!(f, ")")?;

        Ok(())
    }
}
