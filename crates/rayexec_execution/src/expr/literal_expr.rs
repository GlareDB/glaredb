use rayexec_bullet::scalar::{OwnedScalarValue, ScalarValue};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct LiteralExpr {
    pub literal: OwnedScalarValue,
}

impl fmt::Display for LiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.literal {
            ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) => {
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
