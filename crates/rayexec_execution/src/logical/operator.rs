use super::context::QueryContext;
use super::explainable::{ColumnIndexes, ExplainConfig, ExplainEntry, Explainable};
use super::expr::LogicalExpression;
use super::grouping_set::GroupingSets;
use crate::database::create::OnConflict;
use crate::database::drop::DropInfo;
use crate::database::entry::TableEntry;
use crate::engine::vars::SessionVar;
use crate::execution::query_graph::explain::format_logical_plan_for_explain;
use crate::functions::table::InitializedTableFunction;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::{Field, Schema, TypeSchema};
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
    DependentJoin(DependentJoin),
    Limit(Limit),
    MaterializedScan(MaterializedScan),
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
    Describe(Describe),
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
            Self::DependentJoin(n) => n.output_schema(outer),
            Self::Limit(n) => n.output_schema(outer),
            Self::MaterializedScan(n) => n.output_schema(outer),
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
            Self::Describe(n) => n.output_schema(outer),
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
            LogicalOperator::DependentJoin(p) => {
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
            | LogicalOperator::MaterializedScan(_)
            | LogicalOperator::Scan(_)
            | LogicalOperator::Describe(_)
            | LogicalOperator::TableFunction(_) => (),
        }
        post(self)?;

        Ok(())
    }

    /// Return the explain string for a plan. Useful for println debugging.
    #[allow(dead_code)]
    pub(crate) fn debug_explain(&self, context: Option<&QueryContext>) -> String {
        format_logical_plan_for_explain(context, self, ExplainFormat::Text, true).unwrap()
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
            Self::DependentJoin(p) => p.explain_entry(conf),
            Self::Limit(p) => p.explain_entry(conf),
            Self::MaterializedScan(p) => p.explain_entry(conf),
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
            Self::Describe(p) => p.explain_entry(conf),
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
    // TODO: NULL == NULL
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

/// A join where the right input has columns that depend on output in the left.
#[derive(Debug, Clone, PartialEq)]
pub struct DependentJoin {
    pub left: Box<LogicalOperator>,
    pub right: Box<LogicalOperator>,
}

impl LogicalNode for DependentJoin {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let left = self.left.output_schema(outer)?;
        let right = self.right.output_schema(outer)?;
        Ok(left.merge(right))
    }
}

impl Explainable for DependentJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("DependentJoin")
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

/// A scan of a materialized plan.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedScan {
    /// Index of the materialized plan in the query context.
    pub idx: usize,

    /// Compute type schema of the underlying materialized plan.
    // TODO: This currently exists on the operator to avoid needing to pass the
    // QueryContext into `output_schema`. I actually think storing the schema is
    // preferred to what we're currently doing where we compute it every time,
    // but everything else needs to change in order to make it all consistent.
    //
    // I also believe that we can remove the `outer` stuff with my current plan
    // for subqueries, but that's stil tbd.
    pub schema: TypeSchema,
}

impl LogicalNode for MaterializedScan {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(self.schema.clone())
    }
}

impl Explainable for MaterializedScan {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("MaterializedScan")
            .with_value("idx", self.idx)
            .with_values("column_types", &self.schema.types)
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
        let column_types = self.source.columns.iter().map(|c| c.datatype.clone());
        ExplainEntry::new("Scan")
            .with_value("source", &self.source.name)
            .with_values("column_types", column_types)
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

/// An aggregate node containing some number of aggregates, and optional groups.
///
/// The output schema of the this node is [aggregate_columns, group_columns]. A
/// projection above this node should be used to reorder the columns as needed.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    /// Aggregate functions calls.
    ///
    /// During planning, the aggregate function calls will be replaced with
    /// column references.
    pub aggregates: Vec<LogicalExpression>,

    /// Expressions in the GROUP BY clauses.
    ///
    /// Empty indicates we'll be computing an aggregate over a single group.
    pub group_exprs: Vec<LogicalExpression>,

    /// Optional grouping set.
    pub grouping_sets: Option<GroupingSets>,

    /// Input to the aggregate.
    pub input: Box<LogicalOperator>,
}

impl LogicalNode for Aggregate {
    fn output_schema(&self, outer: &[TypeSchema]) -> Result<TypeSchema> {
        let current = self.input.output_schema(outer)?;
        let types = self
            .aggregates
            .iter()
            .chain(self.group_exprs.iter())
            .map(|expr| expr.datatype(&current, outer))
            .collect::<Result<Vec<_>>>()?;

        Ok(TypeSchema::new(types))
    }
}

impl Explainable for Aggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Aggregate")
            .with_values("aggregates", &self.aggregates)
            .with_values("group_exprs", &self.group_exprs);

        if let Some(grouping_set) = &self.grouping_sets {
            ent = ent.with_value("grouping_sets", grouping_set.num_groups());
        }

        ent
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

#[derive(Debug, Clone, PartialEq)]
pub struct Describe {
    pub schema: Schema,
}

impl LogicalNode for Describe {
    fn output_schema(&self, _outer: &[TypeSchema]) -> Result<TypeSchema> {
        Ok(TypeSchema::new(vec![DataType::Utf8, DataType::Utf8]))
    }
}

impl Explainable for Describe {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Describe")
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
