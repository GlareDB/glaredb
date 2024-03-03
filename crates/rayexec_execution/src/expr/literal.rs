

use super::scalar::ScalarValue;



use std::fmt;

/// An expression whose return value is always a literal.
///
/// When evaluating against a record batch, returned array's length will match
/// the number of rows in the batch.
#[derive(Debug, Clone, PartialEq)]
pub struct LiteralExpr {
    pub value: ScalarValue,
}

impl LiteralExpr {
    pub fn new(value: ScalarValue) -> Self {
        LiteralExpr { value }
    }
}

impl fmt::Display for LiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
