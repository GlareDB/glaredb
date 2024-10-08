use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};

use super::Expression;
use std::fmt;

/// <input> BETWEEN <lower> AND <upper>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BetweenExpr {
    pub lower: Box<Expression>,
    pub upper: Box<Expression>,
    pub input: Box<Expression>,
}

impl ContextDisplay for BetweenExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "{} BETWEEN {} AND {}",
            ContextDisplayWrapper::with_mode(self.input.as_ref(), mode),
            ContextDisplayWrapper::with_mode(self.lower.as_ref(), mode),
            ContextDisplayWrapper::with_mode(self.upper.as_ref(), mode),
        )
    }
}
