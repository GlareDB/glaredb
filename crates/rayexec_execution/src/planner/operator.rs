use crate::{
    expr::Expression, functions::table::BoundTableFunction, types::batch::DataBatchSchema,
};

use super::bind_context::BindIdx;

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
    Values(Values),
    Empty(Empty),
}

#[derive(Debug)]
pub struct Projection {
    pub exprs: Vec<Expression>,
    pub input: Box<LogicalOperator>,
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: Expression,
    pub input: Box<LogicalOperator>,
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
pub struct Scan {
    pub projection: Option<Vec<usize>>,
    pub input: BindIdx,
    // TODO: Pushdowns
}

#[derive(Debug)]
pub struct Values {
    pub rows: Vec<Vec<Expression>>,
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

#[derive(Debug)]
pub struct Empty;
