use crate::expr::logical::LogicalExpr;
use rayexec_error::{RayexecError, Result};
use std::hash::Hash;

/// Builds a logical plan for a single query.
#[derive(Debug, Clone, Default)]
pub struct LogicalPlan {
    /// Nodes in this plan.
    pub(crate) nodes: Vec<LogicalPlanNode>,
}

impl LogicalPlan {
    /// Join this plan onto another logical plan.
    pub fn join_on(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        on: Vec<LogicalExpr>,
    ) -> Result<LogicalPlan> {
        if self.nodes.is_empty() {
            return Err(RayexecError::new("Left side of plan empty"));
        }
        if right.nodes.is_empty() {
            return Err(RayexecError::new("Right side of plan empty"));
        }

        unimplemented!()
    }

    /// Cross join this plan with another plan.
    pub fn cross_join(self, right: LogicalPlan) -> Result<LogicalPlan> {
        if self.nodes.is_empty() {
            return Err(RayexecError::new("Left side of plan empty"));
        }
        if right.nodes.is_empty() {
            return Err(RayexecError::new("Right side of plan empty"));
        }

        unimplemented!()
    }

    /// Project the output rows using the provided expressions.
    pub fn project(mut self, exprs: Vec<LogicalExpr>) -> Result<Self> {
        // TODO: Validate exprs

        let input = self.nodes.len();
        let node = LogicalPlanNode::Projection(Projection { exprs, input });

        self.nodes.push(node);
        Ok(self)
    }

    /// Filter the ouput rows using the provided predicate.
    pub fn filter(mut self, predicate: LogicalExpr) -> Result<Self> {
        // TODO: Ensure predicate returns bool.

        let input = self.nodes.len();
        let node = LogicalPlanNode::Filter(Filter { predicate, input });

        self.nodes.push(node);
        Ok(self)
    }

    /// Limit the number of rows returned.
    pub fn limit(mut self, skip: usize, limit: usize) -> Result<LogicalPlan> {
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
pub enum LogicalPlanNode {
    Projection(Projection),
    Filter(Filter),
    Limit(Limit),
    CrossJoin(CrossJoin),
    Join(Join),
    Values(Values),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    /// Output expressions.
    pub exprs: Vec<LogicalExpr>,
    /// Index of the input node.
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Values {
    /// Output expressions.
    pub exprs: Vec<LogicalExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub predicate: LogicalExpr,
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub group_by_exprs: Vec<LogicalExpr>,
    pub agg_exprs: Vec<LogicalExpr>,
    pub input: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub order_by_exprs: Vec<LogicalExpr>,
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
pub struct Join {
    pub left_input: usize,
    pub right_input: usize,
    pub on_exprs: Vec<LogicalExpr>,
    pub join_type: JoinType,
    pub constraint: JoinConstraint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CrossJoin {
    pub left_input: usize,
    pub right_input: usize,
}
