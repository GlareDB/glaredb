use rayexec_parser::ast;

use super::{binary::BinaryExpr, column::ColumnExpr, scalar::ScalarValue};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr<'a> {
    Column(ColumnExpr),
    Literal(ScalarValue),
    Binary(BinaryExpr<'a>),
    Unbound(UnboundExpr<'a>),
}

impl<'a> LogicalExpr<'a> {}

/// An unbound AST expression.
#[derive(Debug, Clone, PartialEq)]
pub struct UnboundExpr<'a>(pub ast::Expr<'a>);
