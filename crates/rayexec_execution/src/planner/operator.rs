use super::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::{
    expr::{
        scalar::{BinaryOperator, ScalarValue, VariadicOperator},
        Expression,
    },
    functions::table::BoundTableFunction,
    types::batch::DataBatchSchema,
};
use rayexec_error::Result;
use rayexec_parser::ast::UnaryOperator;
use std::hash::Hash;

use super::scope::ColumnRef;

#[derive(Debug)]
pub enum LogicalOperator {
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    Order(Order),
    Join(Join),
    CrossJoin(CrossJoin),
    Limit(Limit),
    Scan(Scan),
    ExpressionList(ExpressionList),
    Empty,
}

impl LogicalOperator {
    /// Get the output schema of the operator.
    pub fn schema(&self) -> Result<&DataBatchSchema> {
        match self {
            Self::Scan(scan) => Ok(&scan.schema),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct Projection {
    pub exprs: Vec<Expression>,
    pub input: Box<LogicalOperator>,
}

impl Explainable for Projection {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Projection").with_values(
            "expressions",
            self.exprs.iter().map(|expr| format!("{expr:?}")),
        )
    }
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: LogicalExpression,
    pub input: Box<LogicalOperator>,
}

impl Explainable for Filter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", format!("{:?}", self.predicate))
    }
}

#[derive(Debug)]
pub struct OrderByExpr {
    pub expr: Expression,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug)]
pub struct Order {
    pub exprs: Vec<OrderByExpr>,
    pub input: Box<LogicalOperator>,
}

impl Explainable for Order {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Order").with_values(
            "expressions",
            self.exprs.iter().map(|expr| format!("{expr:?}")),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug)]
pub struct Join {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
    pub join_type: JoinType,
    pub on: Vec<(Expression, Expression)>,
}

#[derive(Debug)]
pub struct CrossJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
}

#[derive(Debug)]
pub struct Limit {
    pub offset: usize,
    pub limit: Option<usize>,
    pub input: Box<LogicalOperator>,
}

#[derive(Debug)]
pub enum ScanItem {
    TableFunction(Box<dyn BoundTableFunction>),
}

#[derive(Debug)]
pub struct Scan {
    pub source: ScanItem,
    pub schema: DataBatchSchema,
    // pub projection: Option<Vec<usize>>,
    // pub input: BindIdx,
    // TODO: Pushdowns
}

#[derive(Debug)]
pub struct ExpressionList {
    pub rows: Vec<Vec<LogicalExpression>>,
    // TODO: Table index.
}

#[derive(Debug)]
pub struct Aggregate {
    pub grouping_expr: GroupingExpr,
    pub agg_exprs: Vec<Expression>,
    pub input: Box<LogicalOperator>,
}

#[derive(Debug)]
pub enum GroupingExpr {
    None,
    GroupingSet(Vec<Expression>),
    Rollup(Vec<Expression>),
    Cube(Vec<Expression>),
    GroupingSets(Vec<Vec<Expression>>),
}

/// An expression that can exist in a logical plan.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpression {
    /// Reference to a column.
    ///
    /// Note that this includes scoping information since this expression can be
    /// part of a correlated subquery.
    ColumnRef(ColumnRef),
    /// Literal value.
    Literal(ScalarValue),
    /// Unary function.
    Unary {
        op: UnaryOperator,
        expr: Box<LogicalExpression>,
    },
    /// Binary function.
    Binary {
        op: BinaryOperator,
        left: Box<LogicalExpression>,
        right: Box<LogicalExpression>,
    },
    /// Variadic function.
    Variadic {
        op: VariadicOperator,
        exprs: Vec<LogicalExpression>,
    },
    /// Case expressions.
    Case {
        input: Box<LogicalExpression>,
        /// When <left>, then <right>
        when_then: Vec<(LogicalExpression, LogicalExpression)>,
    },
}
