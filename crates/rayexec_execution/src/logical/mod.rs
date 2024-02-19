use crate::expr::logical::LogicalExpr;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

/// Builds a logical plan for a single query.
#[derive(Debug, Clone, Default)]
pub struct LogicalPlan<'a> {
    /// Nodes in this plan.
    pub(crate) nodes: Vec<LogicalPlanNode<'a>>,
}

impl<'a> LogicalPlan<'a> {
    /// Construct a logical plan using an empty relation.
    pub fn empty(produce_one: bool) -> Self {
        LogicalPlan {
            nodes: vec![LogicalPlanNode::Empty(Empty { produce_one })],
        }
    }

    /// Construct a logical plan with an unbound table reference.
    pub fn table(reference: ast::ObjectReference<'a>) -> Self {
        LogicalPlan {
            nodes: vec![LogicalPlanNode::UnboundTable(UnboundTable { reference })],
        }
    }

    /// Construct a logical plan with an unbound table function.
    pub fn table_func(reference: ast::ObjectReference<'a>) -> Self {
        LogicalPlan {
            nodes: vec![LogicalPlanNode::UnboundTableFunc(UnboundTableFunc {
                reference,
            })],
        }
    }

    /// Join this plan onto another logical plan.
    pub fn join_on(
        self,
        right: LogicalPlan<'a>,
        join_type: JoinType,
        on: Vec<LogicalExpr<'a>>,
    ) -> Result<Self> {
        if self.nodes.is_empty() {
            return Err(RayexecError::new("Left side of plan empty"));
        }
        if right.nodes.is_empty() {
            return Err(RayexecError::new("Right side of plan empty"));
        }

        unimplemented!()
    }

    /// Cross join this plan with another plan.
    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        if self.nodes.is_empty() {
            return Err(RayexecError::new("Left side of plan empty"));
        }
        if right.nodes.is_empty() {
            return Err(RayexecError::new("Right side of plan empty"));
        }

        unimplemented!()
    }

    /// Project the output rows using the provided expressions.
    pub fn project(mut self, exprs: Vec<LogicalExpr<'a>>) -> Result<Self> {
        // TODO: Validate exprs

        let input = self.nodes.len();
        let node = LogicalPlanNode::Projection(Projection { exprs, input });

        self.nodes.push(node);
        Ok(self)
    }

    /// Filter the ouput rows using the provided predicate.
    pub fn filter(mut self, predicate: LogicalExpr<'a>) -> Result<Self> {
        // TODO: Ensure predicate returns bool.

        let input = self.nodes.len();
        let node = LogicalPlanNode::Filter(Filter { predicate, input });

        self.nodes.push(node);
        Ok(self)
    }

    /// Limit the number of rows returned.
    pub fn limit(mut self, skip: usize, limit: usize) -> Result<Self> {
        if self.nodes.is_empty() {
            return Err(RayexecError::new(
                "No nodes in logical plan, nothing to limit",
            ));
        }
        let input = self.nodes.len();
        let node = LogicalPlanNode::Limit(Limit { skip, limit, input });

        self.nodes.push(node);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlanNode<'a> {
    Projection(Projection<'a>),
    Filter(Filter<'a>),
    Limit(Limit),
    CrossJoin(CrossJoin),
    Join(Join<'a>),
    Values(Values<'a>),
    Empty(Empty),
    UnboundTable(UnboundTable<'a>),
    UnboundTableFunc(UnboundTableFunc<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Projection<'a> {
    /// Output expressions.
    pub exprs: Vec<LogicalExpr<'a>>,
    /// Index of the input node.
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Values<'a> {
    /// Output expressions.
    pub exprs: Vec<LogicalExpr<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Empty {
    pub produce_one: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter<'a> {
    pub predicate: LogicalExpr<'a>,
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate<'a> {
    pub group_by_exprs: Vec<LogicalExpr<'a>>,
    pub agg_exprs: Vec<LogicalExpr<'a>>,
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy<'a> {
    pub order_by_exprs: Vec<LogicalExpr<'a>>,
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub skip: usize,
    pub limit: usize,
    pub input: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    LeftSemi,
    LeftAnti,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinConstraint {
    On,
    Using,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join<'a> {
    pub left_input: usize,
    pub right_input: usize,
    pub on_exprs: Vec<LogicalExpr<'a>>,
    pub join_type: JoinType,
    pub constraint: JoinConstraint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CrossJoin {
    pub left_input: usize,
    pub right_input: usize,
}

/// An unbound table element. This can represent a normal table, or a CTE.
#[derive(Debug, Clone, PartialEq)]
pub struct UnboundTable<'a> {
    pub reference: ast::ObjectReference<'a>,
    // TODO: Alias, column aliases
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnboundTableFunc<'a> {
    pub reference: ast::ObjectReference<'a>,
    // TODO: Args, alias, column aliases.
}
