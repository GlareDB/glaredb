use super::explainable::{ColumnIndexes, ExplainConfig, ExplainEntry, Explainable};
use super::scope::ColumnRef;
use crate::functions::aggregate::GenericAggregateFunction;
use crate::functions::scalar::{GenericScalarFunction, SpecializedScalarFunction};
use crate::{
    engine::vars::SessionVar,
    expr::scalar::{BinaryOperator, UnaryOperator, VariadicOperator},
};
use rayexec_bullet::field::{DataType, TypeSchema};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::fmt;

pub trait LogicalNode {
    /// Get the output type schema of the operator.
    ///
    /// Since we're working with possibly correlated columns, this also accepts
    /// the schema of the outer scopes.
    ///
    /// During physical planning, it's assumed that the logical plan has been
    /// completely decorrelated, and as such will be provided and empty outer
    /// schema.
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema>;
}

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
    /// Get the output type schema of the operator.
    ///
    /// Since we're working with possibly correlated columns, this also accepts
    /// the schema of the outer scopes.
    pub fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        match self {
            Self::Projection(n) => n.output_schema(outer),
            Self::Filter(n) => n.output_schema(outer),
            Self::Aggregate(n) => n.output_schema(outer),
            Self::Order(n) => n.output_schema(outer),
            Self::AnyJoin(n) => n.output_schema(outer),
            Self::EqualityJoin(n) => n.output_schema(outer),
            Self::CrossJoin(n) => n.output_schema(outer),
            Self::Limit(n) => n.output_schema(outer),
            Self::Scan(n) => n.output_schema(outer),
            Self::ExpressionList(n) => n.output_schema(outer),
            Self::Empty => Ok(TypeSchema::empty()),
            Self::SetVar(n) => n.output_schema(outer),
            Self::ShowVar(n) => n.output_schema(outer),
            Self::CreateTableAs(_) => unimplemented!(),
        }
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

impl LogicalNode for Projection {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let current = self.input.output_schema(outer)?;
        let types = self
            .exprs
            .iter()
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;
        Ok(TypeSchema::new(types))
    }
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

impl LogicalNode for Filter {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        // Filter just filters out rows, no column changes happen.
        self.input.output_schema(outer)
    }
}

impl Explainable for Filter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", format!("{}", self.predicate))
    }
}

#[derive(Debug)]
pub struct OrderByExpr {
    pub expr: LogicalExpression,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Debug)]
pub struct Order {
    pub exprs: Vec<OrderByExpr>,
    pub input: Box<LogicalOperator>,
}

impl LogicalNode for Order {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        // Order by doesn't change row output.
        self.input.output_schema(outer)
    }
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

impl LogicalNode for AnyJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
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

impl LogicalNode for EqualityJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
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

impl LogicalNode for CrossJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
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

impl LogicalNode for Limit {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        self.input.output_schema(outer)
    }
}

impl Explainable for Limit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Limit")
    }
}

#[derive(Debug)]
pub enum ScanItem {
    TableFunction(),
}

impl Explainable for ScanItem {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
        // match self {
        //     Self::TableFunction(func) => func.explain_entry(conf),
        // }
    }
}

#[derive(Debug)]
pub struct Scan {
    pub source: ScanItem,
    pub schema: TypeSchema,
    // pub projection: Option<Vec<usize>>,
    // pub input: BindIdx,
    // TODO: Pushdowns
}

impl LogicalNode for Scan {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(self.schema.clone())
    }
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

impl LogicalNode for ExpressionList {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let first = self
            .rows
            .first()
            .ok_or_else(|| RayexecError::new("Expression list contains no rows"))?;
        // No inputs to expression list. Attempting to reference a
        // column should error.
        let current = TypeSchema::empty();
        let types = first
            .iter()
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;
        Ok(TypeSchema::new(types))
    }
}

impl Explainable for ExpressionList {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ExpressionList")
    }
}

#[derive(Debug)]
pub struct Aggregate {
    pub exprs: Vec<LogicalExpression>,
    pub grouping_expr: GroupingExpr,
    pub input: Box<LogicalOperator>,
}

impl LogicalNode for Aggregate {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let current = self.input.output_schema(outer)?;
        let types = self
            .exprs
            .iter()
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;
        Ok(TypeSchema::new(types))
    }
}

impl Explainable for Aggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Aggregate")
    }
}

#[derive(Debug)]
pub enum GroupingExpr {
    /// No grouping expression.
    None,
    /// Group by a single set of columns.
    GroupBy(Vec<LogicalExpression>),
    /// Group by a column rollup.
    Rollup(Vec<LogicalExpression>),
    /// Group by a powerset of the columns.
    Cube(Vec<LogicalExpression>),
}

impl GroupingExpr {
    /// Get mutable references to the input expressions.
    ///
    /// This is used to allow modifying the expression to point to a
    /// pre-projection into the aggregate.
    pub fn expressions_mut(&mut self) -> &mut [LogicalExpression] {
        match self {
            Self::None => &mut [],
            Self::GroupBy(ref mut exprs) => exprs.as_mut_slice(),
            Self::Rollup(ref mut exprs) => exprs.as_mut_slice(),
            Self::Cube(ref mut exprs) => exprs.as_mut_slice(),
        }
    }
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
    pub value: OwnedScalarValue,
}

impl LogicalNode for SetVar {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
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

impl LogicalNode for ShowVar {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        // TODO: Double check with postgres if they convert everything to
        // string in SHOW. I'm adding this in right now just to quickly
        // check the vars for debugging.
        Ok(TypeSchema::new(vec![DataType::Utf8]))
    }
}

impl Explainable for ShowVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ShowVar")
    }
}

/// An expression that can exist in a logical plan.
#[derive(Debug, Clone)]
pub enum LogicalExpression {
    /// Reference to a column.
    ///
    /// Note that this includes scoping information since this expression can be
    /// part of a correlated subquery.
    ColumnRef(ColumnRef),

    /// Literal value.
    Literal(OwnedScalarValue),

    ScalarFunction {
        function: Box<dyn GenericScalarFunction>,
        inputs: Vec<LogicalExpression>,
    },

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
        agg: Box<dyn GenericAggregateFunction>,

        /// Input expressions to the aggragate.
        inputs: Vec<LogicalExpression>,

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
            Self::ScalarFunction {
                function, inputs, ..
            } => write!(
                f,
                "{}({})",
                function.name(),
                inputs
                    .iter()
                    .map(|input| input.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
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
    pub fn datatype(&self, current: &TypeSchema, outer: &[TypeSchema]) -> Result<DataType> {
        Ok(match self {
            LogicalExpression::ColumnRef(col) => {
                if col.scope_level == 0 {
                    // Get data type from current schema.
                    current.types.get(col.item_idx).cloned().ok_or_else(|| {
                        RayexecError::new(format!(
                            "Column reference '{}' points to outside of current schema: {current:?}",
                            col.item_idx
                        ))
                    })?
                } else {
                    // Get data type from one of the outer schemas.
                    outer
                        .get(col.scope_level - 1)
                        .ok_or_else(|| {
                            RayexecError::new("Column reference points to non-existent schema")
                        })?
                        .types
                        .get(col.item_idx)
                        .cloned()
                        .ok_or_else(|| {
                            RayexecError::new("Column reference points to outside of outer schema")
                        })?
                }
            }
            LogicalExpression::Literal(lit) => lit.datatype(),
            LogicalExpression::ScalarFunction { function, inputs } => {
                let datatypes = inputs
                    .iter()
                    .map(|input| input.datatype(current, outer))
                    .collect::<Result<Vec<_>>>()?;
                let ret_type = function.return_type_for_inputs(&datatypes).ok_or_else(|| {
                    RayexecError::new(format!(
                        "Failed to find correct signature for '{}'",
                        function.name()
                    ))
                })?;
                ret_type
            }
            LogicalExpression::Aggregate {
                agg,
                inputs,
                filter: _,
            } => {
                let datatypes = inputs
                    .iter()
                    .map(|input| input.datatype(current, outer))
                    .collect::<Result<Vec<_>>>()?;
                let ret_type = agg.return_type_for_inputs(&datatypes).ok_or_else(|| {
                    RayexecError::new(format!(
                        "Failed to find correct signature for '{}'",
                        agg.name()
                    ))
                })?;
                ret_type
            }
            LogicalExpression::Unary { op: _, expr: _ } => unimplemented!(),
            LogicalExpression::Binary { op, left, right } => {
                let left = left.datatype(current, outer)?;
                let right = right.datatype(current, outer)?;
                let ret_type = op
                    .scalar_function()
                    .return_type_for_inputs(&[left, right])
                    .ok_or_else(|| {
                        RayexecError::new("Failed to get correct signature for scalar function")
                    })?;
                ret_type
            }
            _ => unimplemented!(),
        })
    }

    /// Check if this expression is an aggregate.
    pub const fn is_aggregate(&self) -> bool {
        matches!(self, LogicalExpression::Aggregate { .. })
    }

    /// Try to get a top-level literal from this expression, erroring if it's
    /// not one.
    pub fn try_into_scalar(self) -> Result<OwnedScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit),
            other => Err(RayexecError::new(format!("Not a literal: {other:?}"))),
        }
    }

    pub fn try_into_column_ref(self) -> Result<ColumnRef> {
        match self {
            Self::ColumnRef(col) => Ok(col),
            other => Err(RayexecError::new(format!(
                "Not a column reference: {other:?}"
            ))),
        }
    }
}
