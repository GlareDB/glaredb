use super::Expression;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsOperator {
    IsTrue,
    IsFalse,
    IsNull,
    IsNotNull,
}

impl fmt::Display for IsOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IsTrue => write!(f, "IS TRUE"),
            Self::IsFalse => write!(f, "IS FALSE"),
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IsExpr {
    pub op: IsOperator,
    pub input: Box<Expression>,
}

impl fmt::Display for IsExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.input, self.op)
    }
}
