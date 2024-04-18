use super::explainable::{ColumnIndexes, ExplainConfig, ExplainEntry, Explainable};
use super::scope::ColumnRef;
use crate::functions::aggregate::AggregateFunction;
use crate::{
    engine::vars::SessionVar,
    expr::{
        scalar::{BinaryOperator, ScalarValue, UnaryOperator, VariadicOperator},
        Expression,
    },
    functions::table::BoundTableFunction,
    optimizer::walk_plan,
    types::batch::DataBatchSchema,
};
use arrow_schema::DataType;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::fmt;

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
    SetVar(SetVar),
    ShowVar(ShowVar),
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
            Self::SetVar(_) => DataBatchSchema::empty(),
            // TODO: Double check with postgres if they convert everything to
            // string in SHOW. I'm adding this in right now just to quickly
            // check the vars for debugging.
            Self::ShowVar(_show_var) => DataBatchSchema::new(vec![DataType::Utf8]),
            Self::CreateTableAs(_) => unimplemented!(),
        })
    }

    pub fn as_explain_string(&self) -> Result<String> {
        use std::fmt::Write as _;
        fn write(p: &dyn Explainable, indent: usize, buf: &mut String) -> Result<()> {
            write!(
                buf,
                "{}{}\n",
                std::iter::repeat(" ").take(indent).collect::<String>(),
                p.explain_entry(ExplainConfig { verbose: true })
            )
            .context("failed to write explain string to buffer")?;
            Ok(())
        }

        fn inner(plan: &LogicalOperator, indent: usize, buf: &mut String) -> Result<()> {
            write(plan, indent, buf)?;
            match plan {
                LogicalOperator::Projection(p) => inner(&p.input, indent + 2, buf),
                LogicalOperator::Filter(p) => inner(&p.input, indent + 2, buf),
                LogicalOperator::Aggregate(p) => inner(&p.input, indent + 2, buf),
                LogicalOperator::Order(p) => inner(&p.input, indent + 2, buf),
                LogicalOperator::AnyJoin(p) => {
                    inner(&p.left, indent + 2, buf)?;
                    inner(&p.right, indent + 2, buf)?;
                    Ok(())
                }
                LogicalOperator::EqualityJoin(p) => {
                    inner(&p.left, indent + 2, buf)?;
                    inner(&p.right, indent + 2, buf)?;
                    Ok(())
                }
                LogicalOperator::CrossJoin(p) => {
                    inner(&p.left, indent + 2, buf)?;
                    inner(&p.right, indent + 2, buf)?;
                    Ok(())
                }
                LogicalOperator::Limit(p) => inner(&p.input, indent + 2, buf),
                LogicalOperator::CreateTableAs(p) => inner(&p.input, indent + 2, buf),
                LogicalOperator::Scan(p) => write(&p.source, indent + 2, buf),
                LogicalOperator::Empty
                | LogicalOperator::ExpressionList(_)
                | LogicalOperator::SetVar(_)
                | LogicalOperator::ShowVar(_) => Ok(()),
            }
        }

        let mut buf = String::new();
        inner(self, 0, &mut buf)?;

        Ok(buf)
    }
}

impl Explainable for LogicalOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        match self {
            Self::Projection(p) => p.explain_entry(conf),
            Self::Filter(p) => p.explain_entry(conf),
            Self::Aggregate(p) => p.explain_entry(conf),
            Self::Order(p) => p.explain_entry(conf),
            Self::AnyJoin(p) => p.explain_entry(conf),
            Self::EqualityJoin(p) => p.explain_entry(conf),
            Self::CrossJoin(p) => p.explain_entry(conf),
            Self::Limit(p) => p.explain_entry(conf),
            Self::Scan(p) => p.explain_entry(conf),
            Self::ExpressionList(p) => p.explain_entry(conf),
            Self::Empty => ExplainEntry::new("Empty"),
            Self::SetVar(p) => p.explain_entry(conf),
            Self::ShowVar(p) => p.explain_entry(conf),
            Self::CreateTableAs(p) => p.explain_entry(conf),
        }
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
            self.exprs.iter().map(|expr| format!("{expr}")),
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
        ExplainEntry::new("Filter").with_value("predicate", format!("{}", self.predicate))
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

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "INNER"),
            Self::Left => write!(f, "LEFT"),
            Self::Right => write!(f, "RIGHT"),
            Self::Full => write!(f, "FULL"),
        }
    }
}

/// A join on an arbitrary expression.
#[derive(Debug)]
pub struct AnyJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
    pub join_type: JoinType,
    pub on: LogicalExpression,
}

impl Explainable for AnyJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("AnyJoin")
            .with_value("join", self.join_type)
            .with_value("on", &self.on)
    }
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

impl Explainable for EqualityJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("EqualityJoin")
            .with_value("join", self.join_type)
            .with_value("left_cols", ColumnIndexes(&self.left_on))
            .with_value("right_cols", ColumnIndexes(&self.right_on))
    }
}

#[derive(Debug)]
pub struct CrossJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
}

impl Explainable for CrossJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CrossJoin")
    }
}

#[derive(Debug)]
pub struct Limit {
    pub offset: Option<usize>,
    pub limit: usize,
    pub input: Box<LogicalOperator>,
}

impl Explainable for Limit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Limit")
    }
}

#[derive(Debug)]
pub enum ScanItem {
    TableFunction(Box<dyn BoundTableFunction>),
}

impl Explainable for ScanItem {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        match self {
            Self::TableFunction(func) => func.explain_entry(conf),
        }
    }
}

#[derive(Debug)]
pub struct Scan {
    pub source: ScanItem,
    pub schema: DataBatchSchema,
    // pub projection: Option<Vec<usize>>,
    // pub input: BindIdx,
    // TODO: Pushdowns
}

impl Explainable for Scan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Scan")
    }
}

#[derive(Debug)]
pub struct ExpressionList {
    pub rows: Vec<Vec<LogicalExpression>>,
    // TODO: Table index.
}

impl Explainable for ExpressionList {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ExpressionList")
    }
}

#[derive(Debug)]
pub struct Aggregate {
    pub grouping_expr: GroupingExpr,
    pub agg_exprs: Vec<Expression>,
    pub input: Box<LogicalOperator>,
}

impl Explainable for Aggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Aggregate")
    }
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

impl Explainable for CreateTableAs {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTableAs")
    }
}

#[derive(Debug)]
pub struct SetVar {
    pub name: String,
    pub value: ScalarValue,
}

impl Explainable for SetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("SetVar")
    }
}

#[derive(Debug)]
pub struct ShowVar {
    pub var: SessionVar,
}

impl Explainable for ShowVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ShowVar")
    }
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
    /// Unary operator.
    Unary {
        op: UnaryOperator,
        expr: Box<LogicalExpression>,
    },
    /// Binary operator.
    Binary {
        op: BinaryOperator,
        left: Box<LogicalExpression>,
        right: Box<LogicalExpression>,
    },
    /// Variadic operator.
    Variadic {
        op: VariadicOperator,
        exprs: Vec<LogicalExpression>,
    },
    /// An aggregate function.
    Aggregate {
        /// The function.
        // agg: Box<dyn AggregateFunction>, // TODO

        /// Input expressions to the aggragate.
        args: Vec<Box<LogicalExpression>>,

        /// Optional filter to the aggregate.
        filter: Option<Box<LogicalExpression>>,
    },
    /// Case expressions.
    Case {
        input: Box<LogicalExpression>,
        /// When <left>, then <right>
        when_then: Vec<(LogicalExpression, LogicalExpression)>,
    },
}

impl fmt::Display for LogicalExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnRef(col) => {
                if col.scope_level == 0 {
                    write!(f, "#{}", col.item_idx)
                } else {
                    write!(f, "{}#{}", col.scope_level, col.item_idx)
                }
            }
            Self::Literal(val) => write!(f, "{val}"),
            Self::Unary { op, expr } => write!(f, "{op}{expr}"),
            Self::Binary { op, left, right } => write!(f, "{left}{op}{right}"),
            Self::Variadic { .. } => write!(f, "VARIADIC TODO"),
            Self::Aggregate { .. } => write!(f, "AGG TODO"),
            Self::Case { .. } => write!(f, "CASE TODO"),
        }
    }
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

    /// Try to get a top-level literal from this expression, erroring if it's
    /// not one.
    pub fn try_into_scalar(self) -> Result<ScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit),
            other => Err(RayexecError::new(format!("Not a literal: {other:?}"))),
        }
    }
}
