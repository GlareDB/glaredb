use rayexec_bullet::scalar::OwnedScalarValue;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct LiteralExpr {
    pub literal: OwnedScalarValue,
}

impl fmt::Display for LiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.literal)
    }
}
