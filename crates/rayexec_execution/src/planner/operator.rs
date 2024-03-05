use super::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::{
    expr::{
        scalar::{BinaryOperator, ScalarValue, UnaryOperator, VariadicOperator},
        Expression,
    },
    functions::table::BoundTableFunction,
    types::batch::DataBatchSchema,
};
use arrow_schema::DataType;
use rayexec_error::{RayexecError, Result};

use super::scope::ColumnRef;

#[derive(Debug)]
pub enum LogicalOperator {
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    Order(Order),
    AnyJoin(AnyJoin),
    EqualityJoin(EqualityJoin),
    CrossJoin(CrossJoin),
    Limit(Limit),
    Scan(Scan),
    ExpressionList(ExpressionList),
    Empty,

    CreateTableAs(CreateTableAs),
}

impl LogicalOperator {
    /// Get the output schema of the operator.
    ///
    /// Since we're working with possibly correlated columns, this also accepts
    /// the schema of the outer scopes.
    pub fn schema(&self, outer: &[DataBatchSchema]) -> Result<DataBatchSchema> {
        Ok(match self {
            Self::Projection(proj) => {
                let current = proj.input.schema(outer)?;
                let types = proj
                    .exprs
                    .iter()
                    .map(|expr| expr.data_type(&current, outer))
                    .collect::<Result<Vec<_>>>()?;
                DataBatchSchema::new(types)
            }
            Self::Filter(filter) => filter.input.schema(outer)?,
            Self::Aggregate(_agg) => unimplemented!(),
            Self::Order(order) => order.input.schema(outer)?,
            Self::AnyJoin(_join) => unimplemented!(),
            Self::EqualityJoin(_join) => unimplemented!(),
            Self::CrossJoin(_cross) => unimplemented!(),
            Self::Limit(limit) => limit.input.schema(outer)?,
            Self::Scan(scan) => scan.schema.clone(),
            Self::ExpressionList(list) => {
                let first = list
                    .rows
                    .first()
                    .ok_or_else(|| RayexecError::new("Expression list contains no rows"))?;
                // No inputs to expression list. Attempting to reference a
                // column should error.
                let current = DataBatchSchema::empty();
                let types = first
                    .iter()
                    .map(|expr| expr.data_type(&current, outer))
                    .collect::<Result<Vec<_>>>()?;
                DataBatchSchema::new(types)
            }
            Self::Empty => DataBatchSchema::empty(),
            Self::CreateTableAs(_) => unimplemented!(),
        })
    }
}

#[derive(Debug)]
pub struct Projection {
    pub exprs: Vec<LogicalExpression>,
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

/// A join on an arbitrary expression.
#[derive(Debug)]
pub struct AnyJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
    pub join_type: JoinType,
    pub on: LogicalExpression,
}

/// A join on left/right column equality.
#[derive(Debug)]
pub struct EqualityJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
    pub join_type: JoinType,
    pub left_on: Vec<usize>,
    pub right_on: Vec<usize>,
    // TODO: Filter
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

/// Dummy create table for testing.
#[derive(Debug)]
pub struct CreateTableAs {
    pub name: String,
    pub input: Box<LogicalOperator>,
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

impl LogicalExpression {
    /// Get the output data type of this expression.
    ///
    /// Since we're working with possibly correlated columns, both the schema of
    /// the scope and the schema of the outer scopes are provided.
    pub fn data_type(
        &self,
        current: &DataBatchSchema,
        outer: &[DataBatchSchema],
    ) -> Result<DataType> {
        Ok(match self {
            LogicalExpression::ColumnRef(col) => {
                if col.scope_level == 0 {
                    // Get data type from current schema.
                    current
                        .get_types()
                        .get(col.item_idx)
                        .cloned()
                        .ok_or_else(|| {
                            RayexecError::new(
                                "Column reference points to outside of current schema",
                            )
                        })?
                } else {
                    // Get data type from one of the outer schemas.
                    outer
                        .get(col.scope_level - 1)
                        .ok_or_else(|| {
                            RayexecError::new("Column reference points to non-existent schema")
                        })?
                        .get_types()
                        .get(col.item_idx)
                        .cloned()
                        .ok_or_else(|| {
                            RayexecError::new("Column reference points to outside of outer schema")
                        })?
                }
            }
            LogicalExpression::Literal(lit) => lit.data_type(),
            LogicalExpression::Unary { op: _, expr: _ } => unimplemented!(),
            LogicalExpression::Binary { op, left, right } => {
                let left = left.data_type(current, outer)?;
                let right = right.data_type(current, outer)?;
                op.data_type(&left, &right)?
            }
            _ => unimplemented!(),
        })
    }
}
