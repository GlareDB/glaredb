use std::fmt;

use crate::arrays::scalar::{BorrowedScalarValue, ScalarValue};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiteralExpr {
    pub literal: ScalarValue,
}

impl ContextDisplay for LiteralExpr {
    fn fmt_using_context(
        &self,
        _mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self.literal {
            BorrowedScalarValue::Utf8(_) => {
                // Quote strings.
                //
                // This shouldn't be put in the normal formatting for scalar
                // values since that's all used when displaying the result
                // output. But the display impl for this is used in the context
                // of printing an expression, and strings should be quoted in
                // that case.
                write!(f, "'{}'", self.literal)
            }
            _ => write!(f, "{}", self.literal),
        }
    }
}
