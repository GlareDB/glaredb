use rayexec_parser::ast;

use super::{binary::BinaryExpr, column::ColumnExpr, scalar::ScalarValue};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr<'a> {
    Column(ColumnExpr),
    Literal(ScalarValue),
    Binary(BinaryExpr<'a>),

    IsTrue(Box<LogicalExpr<'a>>),
    IsNotTrue(Box<LogicalExpr<'a>>),
    IsFalse(Box<LogicalExpr<'a>>),
    IsNotFalse(Box<LogicalExpr<'a>>),
    IsNull(Box<LogicalExpr<'a>>),
    IsNotNull(Box<LogicalExpr<'a>>),

    /// An unbound `ALL` that's found in the `GROUP BY ALL` and `ORDER BY ALL`
    /// statements.
    UnboundAll,
    /// An unbound identifer.
    UnboundIdent(ast::Ident<'a>),
    /// An unbound compound identifier.
    UnboundCompoundIdent(Vec<ast::Ident<'a>>),
    /// An unbound wildcard or qualifed wildcard.
    UnboundWildcard(UnboundWildcard<'a>),
}

impl<'a> LogicalExpr<'a> {}

#[derive(Debug, Clone, PartialEq)]
pub enum UnboundWildcard<'a> {
    /// Normal wildcard with optional exclude and replace clauses.
    Wildcard(ast::Wildcard<'a>),
    /// Qualified wildcard with optional exclude and replace clauses.
    QualifiedWildcard(ast::ObjectReference<'a>, ast::Wildcard<'a>),
}

/// Expressions found in the `ORDER BY` part of a query.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr<'a> {
    pub expr: LogicalExpr<'a>,
    pub asc: bool,
    pub nulls_first: bool,
}
