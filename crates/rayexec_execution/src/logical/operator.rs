use super::binder::bind_context::TableRef;
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
    LogicalArbitraryJoin, LogicalComparisonJoin, LogicalCrossJoin, LogicalMagicJoin,
};
use super::logical_limit::LogicalLimit;
use super::logical_materialization::{LogicalMagicMaterializationScan, LogicalMaterializationScan};
use super::logical_order::LogicalOrder;
use super::logical_project::LogicalProject;
use super::logical_scan::LogicalScan;
use super::logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar};
use super::logical_setop::LogicalSetop;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_error::{RayexecError, Result};
use rayexec_proto::ProtoConv;
use std::fmt;

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
    fn get_output_table_refs(&self) -> Vec<TableRef>;
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
    pub fn get_children_table_refs(&self) -> Vec<TableRef> {
        self.children.iter().fold(Vec::new(), |mut refs, child| {
            refs.append(&mut child.get_output_table_refs());
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

impl<N: Explainable> Explainable for Node<N> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.node
            .explain_entry(conf)
            .with_value("location", self.location)
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
}

impl LogicalOperator {
    pub(crate) const EMPTY: LogicalOperator = LogicalOperator::Empty(Node {
        node: LogicalEmpty,
        location: LocationRequirement::Any,
        children: Vec::new(),
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
        }
    }
}

impl LogicalNode for LogicalOperator {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        match self {
            Self::Invalid => Vec::new(), // Programmer error. Maybe panic?
            LogicalOperator::Project(n) => n.get_output_table_refs(),
            LogicalOperator::Filter(n) => n.get_output_table_refs(),
            LogicalOperator::Distinct(n) => n.get_output_table_refs(),
            LogicalOperator::Scan(n) => n.get_output_table_refs(),
            LogicalOperator::MaterializationScan(n) => n.get_output_table_refs(),
            LogicalOperator::MagicMaterializationScan(n) => n.get_output_table_refs(),
            LogicalOperator::Aggregate(n) => n.get_output_table_refs(),
            LogicalOperator::SetOp(n) => n.get_output_table_refs(),
            LogicalOperator::Empty(n) => n.get_output_table_refs(),
            LogicalOperator::Limit(n) => n.get_output_table_refs(),
            LogicalOperator::Order(n) => n.get_output_table_refs(),
            LogicalOperator::SetVar(n) => n.get_output_table_refs(),
            LogicalOperator::ResetVar(n) => n.get_output_table_refs(),
            LogicalOperator::ShowVar(n) => n.get_output_table_refs(),
            LogicalOperator::AttachDatabase(n) => n.get_output_table_refs(),
            LogicalOperator::DetachDatabase(n) => n.get_output_table_refs(),
            LogicalOperator::Drop(n) => n.get_output_table_refs(),
            LogicalOperator::Insert(n) => n.get_output_table_refs(),
            LogicalOperator::CreateSchema(n) => n.get_output_table_refs(),
            LogicalOperator::CreateTable(n) => n.get_output_table_refs(),
            LogicalOperator::CreateView(n) => n.get_output_table_refs(),
            LogicalOperator::Describe(n) => n.get_output_table_refs(),
            LogicalOperator::Explain(n) => n.get_output_table_refs(),
            LogicalOperator::CopyTo(n) => n.get_output_table_refs(),
            LogicalOperator::CrossJoin(n) => n.get_output_table_refs(),
            LogicalOperator::ArbitraryJoin(n) => n.get_output_table_refs(),
            LogicalOperator::ComparisonJoin(n) => n.get_output_table_refs(),
            LogicalOperator::MagicJoin(n) => n.get_output_table_refs(),
        }
    }
}
