use std::fmt;

use glaredb_error::Result;

use super::binder::bind_context::{BindContext, MaterializationRef};
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;
use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};

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

    /// Helper for determining the output refs for a given node type.
    fn output_refs<T>(self, node: &Node<T>, bind_context: &BindContext) -> Vec<TableRef> {
        let mut table_refs = match self {
            Self::LeftSemi | Self::LeftAnti | Self::LeftMark { .. } => {
                // Join types only emit the left side.
                node.children
                    .first()
                    .map(|c| c.get_output_table_refs(bind_context))
                    .unwrap_or_default()
            }
            _ => node.get_children_table_refs(bind_context),
        };

        if let JoinType::LeftMark { table_ref } = self {
            // Left mark also produces an extra boolean column with its own ref append
            // to the end.
            table_refs.push(table_ref);
        }

        table_refs
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "INNER"),
            Self::Left => write!(f, "LEFT"),
            Self::Right => write!(f, "RIGHT"),
            Self::Full => write!(f, "FULL"),
            Self::LeftSemi => write!(f, "LEFT SEMI"),
            Self::LeftAnti => write!(f, "LEFT ANTI"),
            Self::LeftMark { table_ref } => write!(f, "LEFT MARK (ref = {table_ref})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinCondition {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub op: ComparisonOperator,
}

impl JoinCondition {
    /// Flips the sides of the condition, including flipping the operatator.
    ///
    /// E.g. 'a >= b' becomes 'b <= a'
    pub fn flip_sides(&mut self) {
        self.op = self.op.flip();
        std::mem::swap(&mut self.left, &mut self.right);
    }
}

impl From<ComparisonExpr> for JoinCondition {
    fn from(expr: ComparisonExpr) -> Self {
        JoinCondition {
            left: expr.left,
            right: expr.right,
            op: expr.op,
        }
    }
}

impl ContextDisplay for JoinCondition {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            ContextDisplayWrapper::with_mode(self.left.as_ref(), mode),
            self.op,
            ContextDisplayWrapper::with_mode(self.right.as_ref(), mode),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalComparisonJoin {
    pub join_type: JoinType,
    /// Conditions for the join.
    ///
    /// Left/right references must be preserved. The expression on the left
    /// should only reference the left side. The expression on the right should
    /// reference the right side.
    pub conditions: Vec<JoinCondition>,
}

impl Explainable for LogicalComparisonJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("ComparisonJoin", conf)
            .with_contextual_values("conditions", &self.conditions)
            .with_value("join_type", self.join_type)
            .build()
    }
}

impl LogicalNode for Node<LogicalComparisonJoin> {
    fn name(&self) -> &'static str {
        "ComparisonJoin"
    }

    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.node.join_type.output_refs(self, bind_context)
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        for condition in &self.node.conditions {
            func(&condition.left)?;
            func(&condition.right)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
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
    pub conditions: Vec<JoinCondition>,
}

impl Explainable for LogicalMagicJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("MagicJoin", conf)
            .with_contextual_values("conditions", &self.conditions)
            .with_value("join_type", self.join_type)
            .with_value("materialization_ref", self.mat_ref)
            .build()
    }
}

impl LogicalNode for Node<LogicalMagicJoin> {
    fn name(&self) -> &'static str {
        "MagicJoin"
    }

    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.node.join_type.output_refs(self, bind_context)
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        for condition in &self.node.conditions {
            func(&condition.left)?;
            func(&condition.right)?;
        }
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
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
        EntryBuilder::new("ArbitraryJoin", conf)
            .with_contextual_value("condition", &self.condition)
            .with_value("join_type", self.join_type)
            .build()
    }
}

impl LogicalNode for Node<LogicalArbitraryJoin> {
    fn name(&self) -> &'static str {
        "ArbitraryJoin"
    }

    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.node.join_type.output_refs(self, bind_context)
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        func(&self.node.condition)
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        func(&mut self.node.condition)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalCrossJoin;

impl Explainable for LogicalCrossJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("CrossJoin", conf).build()
    }
}

impl LogicalNode for Node<LogicalCrossJoin> {
    fn name(&self) -> &'static str {
        "CrossJoin"
    }

    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.get_children_table_refs(bind_context)
    }

    fn for_each_expr<'a, F>(&'a self, _func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, _func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        Ok(())
    }
}
