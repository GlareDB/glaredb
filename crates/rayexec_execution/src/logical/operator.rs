use std::fmt;

use rayexec_error::{RayexecError, Result};
use rayexec_proto::ProtoConv;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::logical_aggregate::LogicalAggregate;
use super::logical_attach::{LogicalAttachDatabase, LogicalDetachDatabase};
use super::logical_copy::LogicalCopyTo;
use super::logical_create::{LogicalCreateSchema, LogicalCreateTable, LogicalCreateView};
use super::logical_describe::LogicalDescribe;
use super::logical_distinct::LogicalDistinct;
use super::logical_drop::LogicalDrop;
use super::logical_empty::LogicalEmpty;
use super::logical_explain::LogicalExplain;
use super::logical_filter::LogicalFilter;
use super::logical_insert::LogicalInsert;
use super::logical_join::{
    LogicalArbitraryJoin,
    LogicalComparisonJoin,
    LogicalCrossJoin,
    LogicalMagicJoin,
};
use super::logical_limit::LogicalLimit;
use super::logical_materialization::{LogicalMagicMaterializationScan, LogicalMaterializationScan};
use super::logical_order::LogicalOrder;
use super::logical_project::LogicalProject;
use super::logical_scan::LogicalScan;
use super::logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar};
use super::logical_setop::LogicalSetop;
use super::logical_unnest::LogicalUnnest;
use super::logical_window::LogicalWindow;
use super::statistics::StatisticsValue;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

/// Requirement for where a node in the plan needs to be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationRequirement {
    /// Required to be executed locally on the client.
    ClientLocal,
    /// Required to be executed remotely.
    Remote,
    /// Can be executed either locally or remote.
    ///
    /// Unless explicitly required during binding, all nodes should start with
    /// this variant.
    ///
    /// An optimization pass will walk the plan an flip this to either local or
    /// remote depending on where the node sits in the plan.
    Any,
}

impl ProtoConv for LocationRequirement {
    type ProtoType = rayexec_proto::generated::logical::LocationRequirement;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::ClientLocal => Self::ProtoType::ClientLocal,
            Self::Remote => Self::ProtoType::Remote,
            Self::Any => Self::ProtoType::Any,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidLocationRequirement => {
                return Err(RayexecError::new("invalid"))
            }
            Self::ProtoType::ClientLocal => Self::ClientLocal,
            Self::ProtoType::Remote => Self::Remote,
            Self::ProtoType::Any => Self::Any,
        })
    }
}

impl fmt::Display for LocationRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientLocal => write!(f, "ClientLocal"),
            Self::Remote => write!(f, "Remote"),
            Self::Any => write!(f, "Any"),
        }
    }
}

/// Common operations across all logical nodes in a plan.
///
/// For individual operators, this should be implemented on `Node<T>` and not
/// `T`.
///
/// This is implemented on `LogicalOperator` for convenience.
pub trait LogicalNode {
    /// Returns a list of table refs represent the output of this operator.
    ///
    /// After all planning and optimization, a logical operator should only be
    /// referencing the table refs of its direct children. If this holds, we can
    /// then just generate column indexes when referencing batch columns in
    /// physical operators.
    ///
    /// If a logical operator references a table ref that isn't the output of
    /// any of its immediate children, then we messed up planning (e.g. didn't
    /// fully decorrelate).
    ///
    /// This accepts a bind context for materializations as the materialized
    /// plan (and associated output table refs) exist just in the bind context.
    /// Since table refs may be updated through various stages of
    /// planning/optimizing, we want to avoid caching them directly on the
    /// operator.
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef>;

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>;

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>;
}

/// Wrapper around nodes in the logical plan to holds additional metadata for
/// the node.
#[derive(Debug, Clone, PartialEq)]
pub struct Node<N> {
    /// Node specific logic.
    pub node: N,
    /// Location where this node should be executed.
    ///
    /// May be 'Any' if there's no requirement that this node executes on the
    /// client or server.
    pub location: LocationRequirement,
    /// Inputs to this node.
    pub children: Vec<LogicalOperator>,
    /// Estimated output cardinality of this node.
    ///
    /// Should be intialized to 'unknown'. Various optimizer rules will fill
    /// this in as needed.
    pub estimated_cardinality: StatisticsValue<usize>,
}

impl<N> Node<N> {
    pub fn into_inner(self) -> N {
        self.node
    }

    pub fn take_one_child_exact(&mut self) -> Result<LogicalOperator> {
        if self.children.len() != 1 {
            return Err(RayexecError::new(format!(
                "Expected 1 child to operator, have {}",
                self.children.len()
            )));
        }
        Ok(self.children.pop().unwrap())
    }

    pub fn take_two_children_exact(&mut self) -> Result<[LogicalOperator; 2]> {
        if self.children.len() != 2 {
            return Err(RayexecError::new(format!(
                "Expected 2 children to operator, have {}",
                self.children.len()
            )));
        }

        let second = self.children.pop().unwrap();
        let first = self.children.pop().unwrap();

        Ok([first, second])
    }

    pub fn get_one_child_exact(&self) -> Result<&LogicalOperator> {
        if self.children.len() != 1 {
            return Err(RayexecError::new(format!(
                "Expected 1 child to operator, have {}",
                self.children.len()
            )));
        }
        Ok(&self.children[0])
    }

    pub fn get_nth_child(&self, n: usize) -> Result<&LogicalOperator> {
        if self.children.len() < n + 1 {
            return Err(RayexecError::new(format!(
                "Expected at least {} children, got {}",
                n + 1,
                self.children.len()
            )));
        }

        Ok(&self.children[n])
    }

    pub fn get_nth_child_mut(&mut self, n: usize) -> Result<&mut LogicalOperator> {
        if self.children.len() < n + 1 {
            return Err(RayexecError::new(format!(
                "Expected at least {} children, got {}",
                n + 1,
                self.children.len()
            )));
        }

        Ok(&mut self.children[n])
    }

    /// Get all table refs from the immediate children of this node.
    pub fn get_children_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.children.iter().fold(Vec::new(), |mut refs, child| {
            refs.append(&mut child.get_output_table_refs(bind_context));
            refs
        })
    }

    // TODO: Duplicated with LogicalOperator.
    pub fn modify_replace_children<F>(&mut self, modify: &mut F) -> Result<()>
    where
        F: FnMut(LogicalOperator) -> Result<LogicalOperator>,
    {
        let mut new_children = Vec::with_capacity(self.children.len());

        for child in self.children.drain(..) {
            new_children.push(modify(child)?);
        }

        self.children = new_children;

        Ok(())
    }
}

impl<N> Explainable for Node<N>
where
    N: Explainable,
    Node<N>: LogicalNode,
{
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = self
            .node
            .explain_entry(conf)
            .with_value("location", self.location);

        if conf.verbose {
            ent = ent.with_value("cardinality", self.estimated_cardinality)
        }

        ent
    }
}

impl<N> AsRef<N> for Node<N> {
    fn as_ref(&self) -> &N {
        &self.node
    }
}

impl<N> AsMut<N> for Node<N> {
    fn as_mut(&mut self) -> &mut N {
        &mut self.node
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOperator {
    Invalid,
    Project(Node<LogicalProject>),
    Filter(Node<LogicalFilter>),
    Limit(Node<LogicalLimit>),
    Order(Node<LogicalOrder>),
    Distinct(Node<LogicalDistinct>),
    Aggregate(Node<LogicalAggregate>),
    SetOp(Node<LogicalSetop>),
    Scan(Node<LogicalScan>),
    MaterializationScan(Node<LogicalMaterializationScan>),
    MagicMaterializationScan(Node<LogicalMagicMaterializationScan>),
    Empty(Node<LogicalEmpty>),
    SetVar(Node<LogicalSetVar>),
    ResetVar(Node<LogicalResetVar>),
    ShowVar(Node<LogicalShowVar>),
    AttachDatabase(Node<LogicalAttachDatabase>),
    DetachDatabase(Node<LogicalDetachDatabase>),
    Drop(Node<LogicalDrop>),
    Insert(Node<LogicalInsert>),
    CreateSchema(Node<LogicalCreateSchema>),
    CreateTable(Node<LogicalCreateTable>),
    CreateView(Node<LogicalCreateView>),
    Describe(Node<LogicalDescribe>),
    Explain(Node<LogicalExplain>),
    CopyTo(Node<LogicalCopyTo>),
    CrossJoin(Node<LogicalCrossJoin>),
    ComparisonJoin(Node<LogicalComparisonJoin>),
    ArbitraryJoin(Node<LogicalArbitraryJoin>),
    MagicJoin(Node<LogicalMagicJoin>),
    Unnest(Node<LogicalUnnest>),
    Window(Node<LogicalWindow>),
}

impl LogicalOperator {
    pub(crate) const EMPTY: LogicalOperator = LogicalOperator::Empty(Node {
        node: LogicalEmpty,
        location: LocationRequirement::Any,
        children: Vec::new(),
        estimated_cardinality: StatisticsValue::Unknown,
    });

    pub fn location(&self) -> &LocationRequirement {
        unimplemented!()
    }

    pub fn location_mut(&mut self) -> &mut LocationRequirement {
        unimplemented!()
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::EMPTY)
    }

    pub fn take_boxed(self: &mut Box<Self>) -> Box<Self> {
        std::mem::replace(self, Box::new(Self::EMPTY))
    }

    pub fn for_each_child_mut<F>(&mut self, _f: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        unimplemented!()
    }

    /// Walk the plan depth first.
    ///
    /// `pre` provides access to children on the way down, and `post` on the way
    /// up.
    pub fn walk_mut<F1, F2>(&mut self, _pre: &mut F1, _post: &mut F2) -> Result<()>
    where
        F1: FnMut(&mut LogicalOperator) -> Result<()>,
        F2: FnMut(&mut LogicalOperator) -> Result<()>,
    {
        unimplemented!()
    }

    /// Replaces the children in the operator by running them through `modify`.
    ///
    /// Children will be left in an undetermined state if `modify` errors.
    pub fn modify_replace_children<F>(&mut self, modify: &mut F) -> Result<()>
    where
        F: FnMut(LogicalOperator) -> Result<LogicalOperator>,
    {
        let children = self.children_mut();
        let mut new_children = Vec::with_capacity(children.len());

        for child in children.drain(..) {
            new_children.push(modify(child)?);
        }

        *children = new_children;

        Ok(())
    }

    pub fn children(&self) -> &[LogicalOperator] {
        match self {
            Self::Invalid => panic!("attempting to get children for invalid operator"),
            Self::Project(n) => &n.children,
            Self::Filter(n) => &n.children,
            Self::Distinct(n) => &n.children,
            Self::Scan(n) => &n.children,
            Self::MaterializationScan(n) => &n.children,
            Self::MagicMaterializationScan(n) => &n.children,
            Self::Aggregate(n) => &n.children,
            Self::SetOp(n) => &n.children,
            Self::Empty(n) => &n.children,
            Self::Limit(n) => &n.children,
            Self::Order(n) => &n.children,
            Self::SetVar(n) => &n.children,
            Self::ResetVar(n) => &n.children,
            Self::ShowVar(n) => &n.children,
            Self::AttachDatabase(n) => &n.children,
            Self::DetachDatabase(n) => &n.children,
            Self::Drop(n) => &n.children,
            Self::Insert(n) => &n.children,
            Self::CreateSchema(n) => &n.children,
            Self::CreateTable(n) => &n.children,
            Self::CreateView(n) => &n.children,
            Self::Describe(n) => &n.children,
            Self::Explain(n) => &n.children,
            Self::CopyTo(n) => &n.children,
            Self::CrossJoin(n) => &n.children,
            Self::ArbitraryJoin(n) => &n.children,
            Self::ComparisonJoin(n) => &n.children,
            Self::MagicJoin(n) => &n.children,
            Self::Unnest(n) => &n.children,
            Self::Window(n) => &n.children,
        }
    }

    pub fn children_mut(&mut self) -> &mut Vec<LogicalOperator> {
        match self {
            Self::Invalid => panic!("attempting to get children for invalid operator"),
            Self::Project(n) => &mut n.children,
            Self::Filter(n) => &mut n.children,
            Self::Distinct(n) => &mut n.children,
            Self::Scan(n) => &mut n.children,
            Self::MaterializationScan(n) => &mut n.children,
            Self::MagicMaterializationScan(n) => &mut n.children,
            Self::Aggregate(n) => &mut n.children,
            Self::SetOp(n) => &mut n.children,
            Self::Empty(n) => &mut n.children,
            Self::Limit(n) => &mut n.children,
            Self::Order(n) => &mut n.children,
            Self::SetVar(n) => &mut n.children,
            Self::ResetVar(n) => &mut n.children,
            Self::ShowVar(n) => &mut n.children,
            Self::AttachDatabase(n) => &mut n.children,
            Self::DetachDatabase(n) => &mut n.children,
            Self::Drop(n) => &mut n.children,
            Self::Insert(n) => &mut n.children,
            Self::CreateSchema(n) => &mut n.children,
            Self::CreateTable(n) => &mut n.children,
            Self::CreateView(n) => &mut n.children,
            Self::Describe(n) => &mut n.children,
            Self::Explain(n) => &mut n.children,
            Self::CopyTo(n) => &mut n.children,
            Self::CrossJoin(n) => &mut n.children,
            Self::ArbitraryJoin(n) => &mut n.children,
            Self::ComparisonJoin(n) => &mut n.children,
            Self::MagicJoin(n) => &mut n.children,
            Self::Unnest(n) => &mut n.children,
            Self::Window(n) => &mut n.children,
        }
    }

    pub fn is_project(&self) -> bool {
        matches!(self, LogicalOperator::Project(_))
    }

    pub fn estimated_cardinality(&self) -> StatisticsValue<usize> {
        match self {
            Self::Invalid => panic!("attempted to get statistics for invalid operator"),
            LogicalOperator::Project(n) => n.estimated_cardinality,
            LogicalOperator::Filter(n) => n.estimated_cardinality,
            LogicalOperator::Distinct(n) => n.estimated_cardinality,
            LogicalOperator::Scan(n) => n.estimated_cardinality,
            LogicalOperator::MaterializationScan(n) => n.estimated_cardinality,
            LogicalOperator::MagicMaterializationScan(n) => n.estimated_cardinality,
            LogicalOperator::Aggregate(n) => n.estimated_cardinality,
            LogicalOperator::SetOp(n) => n.estimated_cardinality,
            LogicalOperator::Empty(n) => n.estimated_cardinality,
            LogicalOperator::Limit(n) => n.estimated_cardinality,
            LogicalOperator::Order(n) => n.estimated_cardinality,
            LogicalOperator::SetVar(n) => n.estimated_cardinality,
            LogicalOperator::ResetVar(n) => n.estimated_cardinality,
            LogicalOperator::ShowVar(n) => n.estimated_cardinality,
            LogicalOperator::AttachDatabase(n) => n.estimated_cardinality,
            LogicalOperator::DetachDatabase(n) => n.estimated_cardinality,
            LogicalOperator::Drop(n) => n.estimated_cardinality,
            LogicalOperator::Insert(n) => n.estimated_cardinality,
            LogicalOperator::CreateSchema(n) => n.estimated_cardinality,
            LogicalOperator::CreateTable(n) => n.estimated_cardinality,
            LogicalOperator::CreateView(n) => n.estimated_cardinality,
            LogicalOperator::Describe(n) => n.estimated_cardinality,
            LogicalOperator::Explain(n) => n.estimated_cardinality,
            LogicalOperator::CopyTo(n) => n.estimated_cardinality,
            LogicalOperator::CrossJoin(n) => n.estimated_cardinality,
            LogicalOperator::ArbitraryJoin(n) => n.estimated_cardinality,
            LogicalOperator::ComparisonJoin(n) => n.estimated_cardinality,
            LogicalOperator::MagicJoin(n) => n.estimated_cardinality,
            LogicalOperator::Unnest(n) => n.estimated_cardinality,
            LogicalOperator::Window(n) => n.estimated_cardinality,
        }
    }
}

impl LogicalNode for LogicalOperator {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        match self {
            Self::Invalid => Vec::new(), // Programmer error. Maybe panic?
            LogicalOperator::Project(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Filter(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Distinct(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Scan(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::MaterializationScan(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::MagicMaterializationScan(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Aggregate(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::SetOp(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Empty(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Limit(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Order(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::SetVar(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::ResetVar(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::ShowVar(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::AttachDatabase(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::DetachDatabase(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Drop(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Insert(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::CreateSchema(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::CreateTable(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::CreateView(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Describe(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Explain(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::CopyTo(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::CrossJoin(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::ArbitraryJoin(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::ComparisonJoin(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::MagicJoin(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Unnest(n) => n.get_output_table_refs(bind_context),
            LogicalOperator::Window(n) => n.get_output_table_refs(bind_context),
        }
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        match self {
            Self::Invalid => panic!("attempted to get exprs for invalid operator"),
            LogicalOperator::Project(n) => n.for_each_expr(func),
            LogicalOperator::Filter(n) => n.for_each_expr(func),
            LogicalOperator::Distinct(n) => n.for_each_expr(func),
            LogicalOperator::Scan(n) => n.for_each_expr(func),
            LogicalOperator::MaterializationScan(n) => n.for_each_expr(func),
            LogicalOperator::MagicMaterializationScan(n) => n.for_each_expr(func),
            LogicalOperator::Aggregate(n) => n.for_each_expr(func),
            LogicalOperator::SetOp(n) => n.for_each_expr(func),
            LogicalOperator::Empty(n) => n.for_each_expr(func),
            LogicalOperator::Limit(n) => n.for_each_expr(func),
            LogicalOperator::Order(n) => n.for_each_expr(func),
            LogicalOperator::SetVar(n) => n.for_each_expr(func),
            LogicalOperator::ResetVar(n) => n.for_each_expr(func),
            LogicalOperator::ShowVar(n) => n.for_each_expr(func),
            LogicalOperator::AttachDatabase(n) => n.for_each_expr(func),
            LogicalOperator::DetachDatabase(n) => n.for_each_expr(func),
            LogicalOperator::Drop(n) => n.for_each_expr(func),
            LogicalOperator::Insert(n) => n.for_each_expr(func),
            LogicalOperator::CreateSchema(n) => n.for_each_expr(func),
            LogicalOperator::CreateTable(n) => n.for_each_expr(func),
            LogicalOperator::CreateView(n) => n.for_each_expr(func),
            LogicalOperator::Describe(n) => n.for_each_expr(func),
            LogicalOperator::Explain(n) => n.for_each_expr(func),
            LogicalOperator::CopyTo(n) => n.for_each_expr(func),
            LogicalOperator::CrossJoin(n) => n.for_each_expr(func),
            LogicalOperator::ArbitraryJoin(n) => n.for_each_expr(func),
            LogicalOperator::ComparisonJoin(n) => n.for_each_expr(func),
            LogicalOperator::MagicJoin(n) => n.for_each_expr(func),
            LogicalOperator::Unnest(n) => n.for_each_expr(func),
            LogicalOperator::Window(n) => n.for_each_expr(func),
        }
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        match self {
            Self::Invalid => panic!("attempted to get exprs for invalid operator"),
            LogicalOperator::Project(n) => n.for_each_expr_mut(func),
            LogicalOperator::Filter(n) => n.for_each_expr_mut(func),
            LogicalOperator::Distinct(n) => n.for_each_expr_mut(func),
            LogicalOperator::Scan(n) => n.for_each_expr_mut(func),
            LogicalOperator::MaterializationScan(n) => n.for_each_expr_mut(func),
            LogicalOperator::MagicMaterializationScan(n) => n.for_each_expr_mut(func),
            LogicalOperator::Aggregate(n) => n.for_each_expr_mut(func),
            LogicalOperator::SetOp(n) => n.for_each_expr_mut(func),
            LogicalOperator::Empty(n) => n.for_each_expr_mut(func),
            LogicalOperator::Limit(n) => n.for_each_expr_mut(func),
            LogicalOperator::Order(n) => n.for_each_expr_mut(func),
            LogicalOperator::SetVar(n) => n.for_each_expr_mut(func),
            LogicalOperator::ResetVar(n) => n.for_each_expr_mut(func),
            LogicalOperator::ShowVar(n) => n.for_each_expr_mut(func),
            LogicalOperator::AttachDatabase(n) => n.for_each_expr_mut(func),
            LogicalOperator::DetachDatabase(n) => n.for_each_expr_mut(func),
            LogicalOperator::Drop(n) => n.for_each_expr_mut(func),
            LogicalOperator::Insert(n) => n.for_each_expr_mut(func),
            LogicalOperator::CreateSchema(n) => n.for_each_expr_mut(func),
            LogicalOperator::CreateTable(n) => n.for_each_expr_mut(func),
            LogicalOperator::CreateView(n) => n.for_each_expr_mut(func),
            LogicalOperator::Describe(n) => n.for_each_expr_mut(func),
            LogicalOperator::Explain(n) => n.for_each_expr_mut(func),
            LogicalOperator::CopyTo(n) => n.for_each_expr_mut(func),
            LogicalOperator::CrossJoin(n) => n.for_each_expr_mut(func),
            LogicalOperator::ArbitraryJoin(n) => n.for_each_expr_mut(func),
            LogicalOperator::ComparisonJoin(n) => n.for_each_expr_mut(func),
            LogicalOperator::MagicJoin(n) => n.for_each_expr_mut(func),
            LogicalOperator::Unnest(n) => n.for_each_expr_mut(func),
            LogicalOperator::Window(n) => n.for_each_expr_mut(func),
        }
    }
}
