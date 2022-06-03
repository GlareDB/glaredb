use crate::catalog::{Catalog, CatalogError, ResolvedTableReference, TableReference};
use coretypes::{
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::ScalarExpr,
};
use sqlparser::ast;
use sqlparser::parser::{Parser, ParserError};

#[derive(Debug)]
pub enum RelationalPlan {
    /// Evaluate a filter on all inputs.
    ///
    /// "WHERE ..."
    Filter(Filter),
    /// Project from inputs.
    ///
    /// "SELECT ..."
    Project(Project),
    /// Join two plan nodes.
    Join(Join),
    /// Cross join two nodes.
    CrossJoin(CrossJoin),
    /// A base table scan.
    Scan(Scan),
    /// Constant values.
    Values(Values),
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<RelationalPlan>,
}

#[derive(Debug)]
pub struct Project {
    /// A list of expressions to evaluate. The may introduce new values.
    pub expressions: Vec<ScalarExpr>,
    pub input: Box<RelationalPlan>,
}

#[derive(Debug)]
pub struct Scan {
    pub table: ResolvedTableReference,
    /// Schema describing the table with the projections applied.
    pub projected_schema: RelationSchema,
    /// An optional list of column indices to project.
    pub project: Option<Vec<usize>>,
    /// An optional list of filters to apply during scanning. Expressions should
    /// return booleans indicating if the row should be returned.
    pub filters: Option<Vec<ScalarExpr>>,
}

#[derive(Debug)]
pub struct Values {
    pub schema: RelationSchema,
    pub values: Vec<Vec<ScalarExpr>>,
}

#[derive(Debug)]
pub struct Join {
    pub left: Box<RelationalPlan>,
    pub right: Box<RelationalPlan>,
    pub join_type: JoinType,
    pub operator: JoinOperator,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug)]
pub enum JoinOperator {
    On(ScalarExpr),
}

#[derive(Debug)]
pub struct CrossJoin {
    pub left: Box<RelationalPlan>,
    pub right: Box<RelationalPlan>,
}

impl RelationalPlan {}
