use super::{binary::BinaryExpr, scalar::ScalarValue};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr {
    Literal(ScalarValue),
    Binary(BinaryExpr),
}
