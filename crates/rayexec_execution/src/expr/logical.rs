use super::{binary::BinaryExpr, column::ColumnExpr, scalar::ScalarValue};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr {
    Column(ColumnExpr),
    Literal(ScalarValue),
    Binary(BinaryExpr),
}

impl LogicalExpr {}
