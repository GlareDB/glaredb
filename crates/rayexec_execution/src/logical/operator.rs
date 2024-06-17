use super::explainable::{ColumnIndexes, ExplainConfig, ExplainEntry, Explainable};
use super::sql::scope::ColumnRef;
use crate::database::create::OnConflict;
use crate::database::drop::DropInfo;
use crate::database::entry::TableEntry;
use crate::execution::query_graph::explain::format_logical_plan_for_explain;
use crate::functions::aggregate::GenericAggregateFunction;
use crate::functions::scalar::GenericScalarFunction;
use crate::functions::table::InitializedTableFunction;
use crate::{
    engine::vars::SessionVar,
    expr::scalar::{BinaryOperator, UnaryOperator, VariadicOperator},
};
use rayexec_bullet::field::{DataType, Field, TypeSchema};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
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

#[derive(Debug, Clone, PartialEq)]
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
    TableFunction(TableFunction),
    ExpressionList(ExpressionList),
    Empty,
    SetVar(SetVar),
    ShowVar(ShowVar),
    ResetVar(ResetVar),
    CreateSchema(CreateSchema),
    CreateTable(CreateTable),
    CreateTableAs(CreateTableAs),
    AttachDatabase(AttachDatabase),
    DetachDatabase(DetachDatabase),
    Drop(DropEntry),
    Insert(Insert),
    Explain(Explain),
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
            Self::TableFunction(n) => n.output_schema(outer),
            Self::ExpressionList(n) => n.output_schema(outer),
            Self::Empty => Ok(TypeSchema::empty()),
            Self::SetVar(n) => n.output_schema(outer),
            Self::ShowVar(n) => n.output_schema(outer),
            Self::ResetVar(n) => n.output_schema(outer),
            Self::CreateSchema(n) => n.output_schema(outer),
            Self::CreateTable(n) => n.output_schema(outer),
            Self::CreateTableAs(_) => unimplemented!(),
            Self::AttachDatabase(n) => n.output_schema(outer),
            Self::DetachDatabase(n) => n.output_schema(outer),
            Self::Drop(n) => n.output_schema(outer),
            Self::Insert(n) => n.output_schema(outer),
            Self::Explain(n) => n.output_schema(outer),
        }
    }

    pub fn walk_mut_pre<F>(&mut self, pre: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        self.walk_mut(pre, &mut |_| Ok(()))
    }

    pub fn walk_mut_post<F>(&mut self, post: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        self.walk_mut(&mut |_| Ok(()), post)
    }

    /// Walk the plan depth first.
    ///
    /// `pre` provides access to children on the way down, and `post` on the way
    /// up.
    pub fn walk_mut<F1, F2>(&mut self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(&mut LogicalOperator) -> Result<()>,
        F2: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        pre(self)?;
        match self {
            LogicalOperator::Projection(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::Filter(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::Aggregate(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::Order(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::Limit(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::CrossJoin(p) => {
                pre(&mut p.left)?;
                p.left.walk_mut(pre, post)?;
                post(&mut p.left)?;

                pre(&mut p.right)?;
                p.right.walk_mut(pre, post)?;
                post(&mut p.right)?;
            }
            LogicalOperator::AnyJoin(p) => {
                pre(&mut p.left)?;
                p.left.walk_mut(pre, post)?;
                post(&mut p.left)?;

                pre(&mut p.right)?;
                p.right.walk_mut(pre, post)?;
                post(&mut p.right)?;
            }
            LogicalOperator::EqualityJoin(p) => {
                pre(&mut p.left)?;
                p.left.walk_mut(pre, post)?;
                post(&mut p.left)?;

                pre(&mut p.right)?;
                p.right.walk_mut(pre, post)?;
                post(&mut p.right)?;
            }
            LogicalOperator::CreateTableAs(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::Insert(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::Explain(p) => {
                pre(&mut p.input)?;
                p.input.walk_mut(pre, post)?;
                post(&mut p.input)?;
            }
            LogicalOperator::ExpressionList(_)
            | LogicalOperator::Empty
            | LogicalOperator::SetVar(_)
            | LogicalOperator::ShowVar(_)
            | LogicalOperator::ResetVar(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::AttachDatabase(_)
            | LogicalOperator::DetachDatabase(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::Scan(_)
            | LogicalOperator::TableFunction(_) => (),
        }
        post(self)?;

        Ok(())
    }

    /// Return the explain string for a plan. Useful for println debugging.
    #[allow(dead_code)]
    pub(crate) fn debug_explain(&self) -> String {
        format_logical_plan_for_explain(self, ExplainFormat::Text, true).unwrap()
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
            Self::TableFunction(p) => p.explain_entry(conf),
            Self::ExpressionList(p) => p.explain_entry(conf),
            Self::Empty => ExplainEntry::new("Empty"),
            Self::SetVar(p) => p.explain_entry(conf),
            Self::ShowVar(p) => p.explain_entry(conf),
            Self::ResetVar(p) => p.explain_entry(conf),
            Self::CreateSchema(p) => p.explain_entry(conf),
            Self::CreateTable(p) => p.explain_entry(conf),
            Self::CreateTableAs(p) => p.explain_entry(conf),
            Self::AttachDatabase(n) => n.explain_entry(conf),
            Self::DetachDatabase(n) => n.explain_entry(conf),
            Self::Drop(p) => p.explain_entry(conf),
            Self::Insert(p) => p.explain_entry(conf),
            Self::Explain(p) => p.explain_entry(conf),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: LogicalExpression,
    pub desc: bool,
    pub nulls_first: bool,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.desc { "DESC" } else { "ASC" },
            if self.nulls_first {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
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
            self.exprs.iter().map(|expr| format!("{expr}")),
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
#[derive(Debug, PartialEq, Clone)]
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
#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub catalog: String,
    pub schema: String,
    pub source: TableEntry,
    // pub projection: Option<Vec<usize>>,
    // pub input: BindIdx,
    // TODO: Pushdowns
}

impl LogicalNode for Scan {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        let schema = TypeSchema::new(self.source.columns.iter().map(|f| f.datatype.clone()));
        Ok(schema)
    }
}

impl Explainable for Scan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Scan").with_value("source", &self.source.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableFunction {
    pub function: Box<dyn InitializedTableFunction>,
}

impl LogicalNode for TableFunction {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(self.function.schema().type_schema())
    }
}

impl Explainable for TableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TableFunction")
            .with_value("function", self.function.specialized().name())
    }
}

#[derive(Debug, Clone, PartialEq)]
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
        let mut ent = ExplainEntry::new("ExpressionList");
        for (idx, row) in self.rows.iter().enumerate() {
            ent = ent.with_values(format!("row{idx}"), row);
        }
        ent
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub exprs: Vec<LogicalExpression>,
    pub grouping_expr: Option<GroupingExpr>,
    pub input: Box<LogicalOperator>,
}

impl LogicalNode for Aggregate {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let current = self.input.output_schema(outer)?;
        let mut types = self
            .exprs
            .iter()
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;

        let mut grouping_types = match &self.grouping_expr {
            Some(grouping) => grouping
                .expressions()
                .iter()
                .map(|expr| expr.datatype(&current, outer))
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };

        types.append(&mut grouping_types);

        Ok(TypeSchema::new(types))
    }
}

impl Explainable for Aggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut outputs = self.exprs.clone();
        if let Some(grouping) = &self.grouping_expr {
            outputs.extend(grouping.expressions().iter().cloned());
        }

        let mut ent = ExplainEntry::new("Aggregate").with_values("outputs", &outputs);
        match self.grouping_expr.as_ref() {
            Some(GroupingExpr::GroupBy(exprs)) => ent = ent.with_values("GROUP BY", exprs),
            Some(GroupingExpr::Rollup(exprs)) => ent = ent.with_values("ROLLUP", exprs),
            Some(GroupingExpr::Cube(exprs)) => ent = ent.with_values("CUBE", exprs),
            None => (),
        }
        ent
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupingExpr {
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
            Self::GroupBy(ref mut exprs) => exprs.as_mut_slice(),
            Self::Rollup(ref mut exprs) => exprs.as_mut_slice(),
            Self::Cube(ref mut exprs) => exprs.as_mut_slice(),
        }
    }

    pub fn expressions(&self) -> &[LogicalExpression] {
        match self {
            Self::GroupBy(ref exprs) => exprs.as_slice(),
            Self::Rollup(ref exprs) => exprs.as_slice(),
            Self::Cube(ref exprs) => exprs.as_slice(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchema {
    pub catalog: String,
    pub name: String,
    pub on_conflict: OnConflict,
}

impl LogicalNode for CreateSchema {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for CreateSchema {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateSchema").with_value("name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
    /// Optional input for CREATE TABLE AS
    pub input: Option<Box<LogicalOperator>>,
}

impl LogicalNode for CreateTable {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for CreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable")
            .with_values("columns", self.columns.iter().map(|c| &c.name))
    }
}

/// Dummy create table for testing.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableAs {
    pub name: String,
    pub input: Box<LogicalOperator>,
}

impl Explainable for CreateTableAs {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTableAs")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttachDatabase {
    pub datasource: String,
    pub name: String,
    pub options: HashMap<String, OwnedScalarValue>,
}

impl LogicalNode for AttachDatabase {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for AttachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("AttachDatabase")
            .with_value("datasource", &self.datasource)
            .with_value("name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DetachDatabase {
    pub name: String,
}

impl LogicalNode for DetachDatabase {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for DetachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("DetachDatabase").with_value("name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropEntry {
    pub info: DropInfo,
}

impl LogicalNode for DropEntry {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for DropEntry {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Drop")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: TableEntry,
    pub input: Box<LogicalOperator>,
}

impl LogicalNode for Insert {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        // TODO: This would be someting in the case of RETURNING.
        Ok(TypeSchema::empty())
    }
}

impl Explainable for Insert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert")
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub enum VariableOrAll {
    Variable(SessionVar),
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResetVar {
    pub var: VariableOrAll,
}

impl LogicalNode for ResetVar {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::empty())
    }
}

impl Explainable for ResetVar {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ResetVar")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Explain {
    pub analyze: bool,
    pub verbose: bool,
    pub format: ExplainFormat,
    pub input: Box<LogicalOperator>,
}

impl LogicalNode for Explain {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::new(vec![DataType::Utf8, DataType::Utf8]))
    }
}

impl Explainable for Explain {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Explain")
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
    Literal(OwnedScalarValue),

    /// A function that returns a single value.
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

    /// A scalar subquery.
    Subquery(Box<LogicalOperator>),

    /// An exists/not exists subquery.
    Exists {
        not_exists: bool,
        subquery: Box<LogicalOperator>,
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
            Self::Aggregate {
                agg,
                inputs,
                filter,
            } => {
                write!(
                    f,
                    "{}({})",
                    agg.name(),
                    inputs
                        .iter()
                        .map(|input| input.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(filter) = filter {
                    write!(f, " FILTER ({filter})")?;
                }
                Ok(())
            }
            Self::Subquery(_) => write!(f, "SUBQUERY TODO"),
            Self::Exists { .. } => write!(f, "EXISTS TODO"),
            Self::Case { .. } => write!(f, "CASE TODO"),
        }
    }
}

impl LogicalExpression {
    /// Create a new uncorrelated column reference.
    pub fn new_column(col: usize) -> Self {
        LogicalExpression::ColumnRef(ColumnRef {
            scope_level: 0,
            item_idx: col,
        })
    }

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

                function.return_type_for_inputs(&datatypes).ok_or_else(|| {
                    RayexecError::new(format!(
                        "Failed to find correct signature for '{}'",
                        function.name()
                    ))
                })?
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

                agg.return_type_for_inputs(&datatypes).ok_or_else(|| {
                    RayexecError::new(format!(
                        "Failed to find correct signature for '{}'",
                        agg.name()
                    ))
                })?
            }
            LogicalExpression::Unary { op: _, expr: _ } => unimplemented!(),
            LogicalExpression::Binary { op, left, right } => {
                let left = left.datatype(current, outer)?;
                let right = right.datatype(current, outer)?;

                op.scalar_function()
                    .return_type_for_inputs(&[left, right])
                    .ok_or_else(|| {
                        RayexecError::new("Failed to get correct signature for scalar function")
                    })?
            }
            LogicalExpression::Subquery(subquery) => {
                // TODO: Do we just need outer, or do we want current + outer?
                let mut schema = subquery.output_schema(outer)?;
                match schema.types.len() {
                    1 => schema.types.pop().unwrap(),
                    other => {
                        return Err(RayexecError::new(format!(
                            "Scalar subqueries should return 1 value, got {other}",
                        )))
                    }
                }
            }
            _ => unimplemented!(),
        })
    }

    pub fn walk_mut_pre<F>(&mut self, pre: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        self.walk_mut(pre, &mut |_| Ok(()))
    }

    pub fn walk_mut_post<F>(&mut self, post: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        self.walk_mut(&mut |_| Ok(()), post)
    }

    /// Walk the expression depth first.
    ///
    /// `pre` provides access to children on the way down, and `post` on the way
    /// up.
    pub fn walk_mut<F1, F2>(&mut self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(&mut LogicalExpression) -> Result<()>,
        F2: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        /// Helper function for walking a subquery in an expression.
        fn walk_subquery<F1, F2>(
            plan: &mut LogicalOperator,
            pre: &mut F1,
            post: &mut F2,
        ) -> Result<()>
        where
            F1: FnMut(&mut LogicalExpression) -> Result<()>,
            F2: FnMut(&mut LogicalExpression) -> Result<()>,
        {
            match plan {
                LogicalOperator::Projection(p) => {
                    LogicalExpression::walk_mut_many(&mut p.exprs, pre, post)?
                }
                LogicalOperator::Filter(p) => p.predicate.walk_mut(pre, post)?,
                LogicalOperator::Aggregate(p) => {
                    LogicalExpression::walk_mut_many(&mut p.exprs, pre, post)?;
                    match &mut p.grouping_expr {
                        Some(GroupingExpr::GroupBy(v)) => {
                            LogicalExpression::walk_mut_many(v.as_mut_slice(), pre, post)?;
                        }
                        Some(GroupingExpr::Rollup(v)) => {
                            LogicalExpression::walk_mut_many(v.as_mut_slice(), pre, post)?;
                        }
                        Some(GroupingExpr::Cube(v)) => {
                            LogicalExpression::walk_mut_many(v.as_mut_slice(), pre, post)?;
                        }
                        _ => (),
                    }
                }
                LogicalOperator::Order(p) => {
                    for expr in &mut p.exprs {
                        expr.expr.walk_mut(pre, post)?;
                    }
                }
                LogicalOperator::AnyJoin(p) => p.on.walk_mut(pre, post)?,
                LogicalOperator::EqualityJoin(_) => (),
                LogicalOperator::CrossJoin(_) => (),
                LogicalOperator::Limit(_) => (),
                LogicalOperator::Scan(_) => (),
                LogicalOperator::TableFunction(_) => (),
                LogicalOperator::ExpressionList(p) => {
                    for row in &mut p.rows {
                        LogicalExpression::walk_mut_many(row, pre, post)?;
                    }
                }
                LogicalOperator::SetVar(_) => (),
                LogicalOperator::ShowVar(_) => (),
                LogicalOperator::ResetVar(_) => (),
                LogicalOperator::Insert(_) => (),
                LogicalOperator::CreateSchema(_) => (),
                LogicalOperator::CreateTable(_) => (),
                LogicalOperator::CreateTableAs(_) => (),
                LogicalOperator::AttachDatabase(_) => (),
                LogicalOperator::DetachDatabase(_) => (),
                LogicalOperator::Explain(_) => (),
                LogicalOperator::Drop(_) => (),
                LogicalOperator::Empty => (),
            }
            Ok(())
        }

        pre(self)?;
        match self {
            LogicalExpression::Unary { expr, .. } => {
                pre(expr)?;
                expr.walk_mut(pre, post)?;
                post(expr)?;
            }
            Self::Binary { left, right, .. } => {
                pre(left)?;
                left.walk_mut(pre, post)?;
                post(left)?;

                pre(right)?;
                right.walk_mut(pre, post)?;
                post(right)?;
            }
            Self::Variadic { exprs, .. } => {
                for expr in exprs.iter_mut() {
                    pre(expr)?;
                    expr.walk_mut(pre, post)?;
                    post(expr)?;
                }
            }
            Self::ScalarFunction { inputs, .. } => {
                for input in inputs.iter_mut() {
                    pre(input)?;
                    input.walk_mut(pre, post)?;
                    post(input)?;
                }
            }
            Self::Aggregate { inputs, .. } => {
                for input in inputs.iter_mut() {
                    pre(input)?;
                    input.walk_mut(pre, post)?;
                    post(input)?;
                }
            }
            Self::ColumnRef(_) | Self::Literal(_) => (),
            Self::Subquery(subquery) | Self::Exists { subquery, .. } => {
                // We only care about the expressions in the plan, so it's
                // sufficient to walk the operators only once on the way down.
                subquery.walk_mut_pre(&mut |plan| walk_subquery(plan, pre, post))?;
            }
            Self::Case { .. } => unimplemented!(),
        }
        post(self)?;

        Ok(())
    }

    fn walk_mut_many<F1, F2>(exprs: &mut [Self], pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(&mut LogicalExpression) -> Result<()>,
        F2: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        for expr in exprs.iter_mut() {
            expr.walk_mut(pre, post)?;
        }
        Ok(())
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

impl AsMut<LogicalExpression> for LogicalExpression {
    fn as_mut(&mut self) -> &mut LogicalExpression {
        self
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::OwnedScalarValue;

    use super::*;

    #[test]
    fn walk_plan_pre_post() {
        let mut plan = LogicalOperator::Projection(Projection {
            exprs: Vec::new(),
            input: Box::new(LogicalOperator::Filter(Filter {
                predicate: LogicalExpression::Literal(OwnedScalarValue::Null),
                input: Box::new(LogicalOperator::Empty),
            })),
        });

        plan.walk_mut(
            &mut |child| {
                match child {
                    LogicalOperator::Projection(proj) => proj
                        .exprs
                        .push(LogicalExpression::Literal(OwnedScalarValue::Int8(1))),
                    LogicalOperator::Filter(_) => {}
                    LogicalOperator::Empty => {}
                    other => panic!("unexpected child {other:?}"),
                }
                Ok(())
            },
            &mut |child| {
                match child {
                    LogicalOperator::Projection(proj) => {
                        assert_eq!(
                            vec![LogicalExpression::Literal(OwnedScalarValue::Int8(1))],
                            proj.exprs
                        );
                        proj.exprs
                            .push(LogicalExpression::Literal(OwnedScalarValue::Int8(2)))
                    }
                    LogicalOperator::Filter(_) => {}
                    LogicalOperator::Empty => {}
                    other => panic!("unexpected child {other:?}"),
                }
                Ok(())
            },
        )
        .unwrap();

        match plan {
            LogicalOperator::Projection(proj) => {
                assert_eq!(
                    vec![
                        LogicalExpression::Literal(OwnedScalarValue::Int8(1)),
                        LogicalExpression::Literal(OwnedScalarValue::Int8(2)),
                    ],
                    proj.exprs
                );
            }
            other => panic!("unexpected root {other:?}"),
        }
    }
}
