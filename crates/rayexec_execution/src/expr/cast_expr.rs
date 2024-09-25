use rayexec_bullet::datatype::DataType;
use std::fmt;

use super::Expression;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastExpr {
    pub to: DataType,
    pub expr: Box<Expression>,
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} TO {})", self.expr, self.to)
    }
}
