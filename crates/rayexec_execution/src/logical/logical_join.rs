use std::fmt;

use glaredb_error::Result;

use super::binder::bind_context::{BindContext, MaterializationRef};
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::comparison_expr::ComparisonExpr;
use crate::expr::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Standard LEFT join.
    Left,
    /// Standard RIGHT join.
    Right,
    /// Standard INNER join.
    Inner,
    /// Standard full/outer join.
    Full,
    /// Left semi join.
    LeftSemi,
    /// Left anti join.
    LeftAnti,
    /// A left join that emits all rows on the left side joined with a column
    /// that indicates if there was a join partner on the right.
    ///
    /// These essentially exposes the left visit bitmaps to other operators.
    ///
    /// Idea taken from duckdb.
    LeftMark {
        /// The table ref to use in logical planning the reference the visit
        /// bitmap output.
        ///
        /// This should have a single column of type bool.
        table_ref: TableRef,
    },
}

impl JoinType {
    /// If the result of a join should be empty if the build input is empty.
    ///
    /// Assumes "left" is the build side.
    pub const fn empty_output_on_empty_build(&self) -> bool {
        match self {
            JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => true,
            JoinType::Full | JoinType::Right | JoinType::LeftMark { .. } => false,
        }
    }

    /// If this join produces all rows from the build side.
    ///
    /// Assumes "left" is the build side.
    pub const fn produce_all_build_side_rows(&self) -> bool {
        match self {
            JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftSemi => true,
            JoinType::Inner | JoinType::Right | JoinType::LeftMark { .. } => false,
        }
    }

    /// Helper for determining the output refs for a given node type.
    fn output_refs<T>(self, node: &Node<T>, bind_context: &BindContext) -> Vec<TableRef> {
        if let JoinType::LeftMark { table_ref } = self {
            let mut refs = node
                .children
                .first()
                .map(|c| c.get_output_table_refs(bind_context))
                .unwrap_or_default();
            refs.push(table_ref);
            refs
        } else {
            node.get_children_table_refs(bind_context)
        }
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "INNER"),
            Self::Left => write!(f, "LEFT"),
            Self::Right => write!(f, "RIGHT"),
            Self::Full => write!(f, "FULL"),
            Self::LeftSemi => write!(f, "SEMI"),
            Self::LeftAnti => write!(f, "ANTI"),
            Self::LeftMark { table_ref } => write!(f, "LEFT MARK (ref = {table_ref})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalComparisonJoin {
    pub join_type: JoinType,
    pub conditions: Vec<ComparisonExpr>,
}

impl Explainable for LogicalComparisonJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ComparisonJoin")
            .with_values_context("conditions", conf, &self.conditions)
            .with_value("join_type", self.join_type)
    }
}

impl LogicalNode for Node<LogicalComparisonJoin> {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.node.join_type.output_refs(self, bind_context)
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for condition in &self.node.conditions {
            func(&condition.left)?;
            func(&condition.right)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for condition in &mut self.node.conditions {
            func(&mut condition.left)?;
            func(&mut condition.right)?;
        }
        Ok(())
    }
}

/// A magic join behaves the same as a comparison join, is only used to
/// demarcate a join node that was created as a result of subquery
/// decorrelation.
///
/// A separate type allows us to more easily run certain optimization steps
/// since we'll have a bit more information.
///
/// The left child will be a materialization scan, and the right child will be a
/// normal operator tree with some number of "magic" materialization scans that
/// read deduplicated values from a materialized plan that's referenced on the
/// left.
///
/// This is never an INNER join, so won't be a part of join reordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalMagicJoin {
    /// The materialization reference for the left child.
    ///
    /// Any "magic" materialization scan we see on the right we can assume was
    /// part of the same decorrelation step that created this node.
    pub mat_ref: MaterializationRef,
    /// The join type, behaves the same as a comparison join.
    pub join_type: JoinType,
    /// Conditions, same as comparison join.
    pub conditions: Vec<ComparisonExpr>,
}

impl Explainable for LogicalMagicJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("MagicJoin")
            .with_values_context("conditions", conf, &self.conditions)
            .with_value("join_type", self.join_type)
    }
}

impl LogicalNode for Node<LogicalMagicJoin> {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.node.join_type.output_refs(self, bind_context)
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        for condition in &self.node.conditions {
            func(&condition.left)?;
            func(&condition.right)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        for condition in &mut self.node.conditions {
            func(&mut condition.left)?;
            func(&mut condition.right)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalArbitraryJoin {
    pub join_type: JoinType,
    pub condition: Expression,
}

impl Explainable for LogicalArbitraryJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ArbitraryJoin")
            .with_value("join_type", self.join_type)
            .with_value_context("condition", conf, &self.condition)
    }
}

impl LogicalNode for Node<LogicalArbitraryJoin> {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.node.join_type.output_refs(self, bind_context)
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        func(&self.node.condition)
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        func(&mut self.node.condition)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCrossJoin;

impl Explainable for LogicalCrossJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CrossJoin")
    }
}

impl LogicalNode for Node<LogicalCrossJoin> {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.get_children_table_refs(bind_context)
    }

    fn for_each_expr<F>(&self, _func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, _func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        Ok(())
    }
}
