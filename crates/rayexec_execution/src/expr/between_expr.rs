use super::Expression;
use std::fmt;

/// <input> BETWEEN <lower> AND <upper>
#[derive(Debug, Clone, PartialEq)]
pub struct BetweenExpr {
    pub lower: Box<Expression>,
    pub upper: Box<Expression>,
    pub input: Box<Expression>,
}

impl fmt::Display for BetweenExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} BETWEEN {} AND {}",
            self.input, self.lower, self.upper
        )
    }
}
