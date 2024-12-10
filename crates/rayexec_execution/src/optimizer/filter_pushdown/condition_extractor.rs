use std::collections::HashSet;
use std::fmt::Debug;

use rayexec_error::{not_implemented, RayexecError, Result};

use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::Expression;
use crate::logical::binder::table_list::TableRef;
use crate::logical::logical_join::{ComparisonCondition, JoinType};
use crate::optimizer::filter_pushdown::split::split_conjunction;

#[derive(Debug, Default)]
pub struct ExtractedConditions {
    /// Join conditions successfully extracted from expressions.
    pub comparisons: Vec<ComparisonCondition>,
    /// Expressions that we could not build a condition for.
    ///
    /// These expressions should filter the output of a join.
    pub arbitrary: Vec<Expression>,
    /// Expressions that only rely on inputs on the left side.
    ///
    /// These should be placed into a filter prior to the join.
    pub left_filter: Vec<Expression>,
    /// Expressions that only rely on inputs on the right side.
    ///
    /// These should be placed into a filter prior to the join.
    pub right_filter: Vec<Expression>,
}

/// Abstraction over either a vec or hash set that contains `TableRef`s.
pub trait TableRefContainer: Debug {
    fn contains_ref(&self, table_ref: &TableRef) -> bool;
}

impl TableRefContainer for Vec<TableRef> {
    fn contains_ref(&self, table_ref: &TableRef) -> bool {
        self.contains(table_ref)
    }
}

impl TableRefContainer for HashSet<TableRef> {
    fn contains_ref(&self, table_ref: &TableRef) -> bool {
        self.contains(table_ref)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprJoinSide {
    Left,
    Right,
    Both,
    None,
}

impl ExprJoinSide {
    pub fn combine(self, other: Self) -> ExprJoinSide {
        match (self, other) {
            (a, Self::None) => a,
            (Self::None, b) => b,
            (Self::Both, _) => Self::Both,
            (_, Self::Both) => Self::Both,
            (Self::Left, Self::Left) => Self::Left,
            (Self::Right, Self::Right) => Self::Right,
            _ => Self::Both,
        }
    }

    pub fn try_from_table_refs<'a, C1, C2>(
        refs: impl IntoIterator<Item = &'a TableRef>,
        left_tables: &C1,
        right_tables: &C2,
    ) -> Result<ExprJoinSide>
    where
        C1: TableRefContainer,
        C2: TableRefContainer,
    {
        let mut side = ExprJoinSide::None;
        for table_ref in refs {
            side = side.combine(Self::try_from_table_ref(
                *table_ref,
                left_tables,
                right_tables,
            )?);
        }

        Ok(side)
    }

    /// Finds the side of a join an expression is referencing.
    ///
    /// Errors if the expression is referencing a column that isn't in any of
    /// the table refs provided.
    pub fn try_from_expr<C1, C2>(
        expr: &Expression,
        left_tables: &C1,
        right_tables: &C2,
    ) -> Result<ExprJoinSide>
    where
        C1: TableRefContainer,
        C2: TableRefContainer,
    {
        fn inner<C1, C2>(
            expr: &Expression,
            left_tables: &C1,
            right_tables: &C2,
            side: ExprJoinSide,
        ) -> Result<ExprJoinSide>
        where
            C1: TableRefContainer,
            C2: TableRefContainer,
        {
            match expr {
                Expression::Column(col) => {
                    ExprJoinSide::try_from_table_ref(col.table_scope, left_tables, right_tables)
                }
                Expression::Subquery(_) => not_implemented!("subquery in join condition"),
                other => {
                    let mut side = side;
                    other.for_each_child(&mut |expr| {
                        let new_side = inner(expr, left_tables, right_tables, side)?;
                        side = new_side.combine(side);
                        Ok(())
                    })?;
                    Ok(side)
                }
            }
        }

        inner(expr, left_tables, right_tables, ExprJoinSide::None)
    }

    fn try_from_table_ref<C1, C2>(
        table_ref: TableRef,
        left_tables: &C1,
        right_tables: &C2,
    ) -> Result<ExprJoinSide>
    where
        C1: TableRefContainer,
        C2: TableRefContainer,
    {
        if left_tables.contains_ref(&table_ref) {
            Ok(ExprJoinSide::Left)
        } else if right_tables.contains_ref(&table_ref) {
            Ok(ExprJoinSide::Right)
        } else {
            Err(RayexecError::new(format!(
                "Table ref is invalid. Left: {left_tables:?}, right: {right_tables:?}, got: {table_ref:?}"
            )))
        }
    }
}

#[derive(Debug)]
pub struct JoinConditionExtractor<'a, C1, C2> {
    pub left_tables: &'a C1,
    pub right_tables: &'a C2,
    pub join_type: JoinType,
}

impl<'a, C1, C2> JoinConditionExtractor<'a, C1, C2>
where
    C1: TableRefContainer,
    C2: TableRefContainer,
{
    pub fn new(left_tables: &'a C1, right_tables: &'a C2, join_type: JoinType) -> Self {
        JoinConditionExtractor {
            left_tables,
            right_tables,
            join_type,
        }
    }

    /// Extracts expressions in join conditions, and pre-join filters.
    ///
    /// Extracted pre-join filters take into account the join type, and so
    /// should be able to be used in a LogicalFilter without additional checks.
    pub fn extract(&self, exprs: Vec<Expression>) -> Result<ExtractedConditions> {
        // Split on AND first.
        let mut split_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            split_conjunction(expr, &mut split_exprs);
        }

        let mut extracted = ExtractedConditions::default();

        for expr in split_exprs {
            let side = ExprJoinSide::try_from_expr(&expr, self.left_tables, self.right_tables)?;

            match side {
                ExprJoinSide::Both => {
                    // If we have a comparison expr, try to split it with each
                    // child expression referencing one side of the join.
                    match expr {
                        Expression::Comparison(ComparisonExpr { left, right, op }) => {
                            let left_side = ExprJoinSide::try_from_expr(
                                &left,
                                self.left_tables,
                                self.right_tables,
                            )?;
                            let right_side = ExprJoinSide::try_from_expr(
                                &right,
                                self.left_tables,
                                self.right_tables,
                            )?;

                            // If left and right expressions don't reference both
                            // sides, we can create a join condition for this.
                            if left_side != ExprJoinSide::Both && right_side != ExprJoinSide::Both {
                                debug_assert_ne!(left_side, right_side);

                                let mut condition = ComparisonCondition {
                                    left: *left,
                                    right: *right,
                                    op,
                                };

                                // If left expression is actually referencing right
                                // side, flip the condition.
                                if left_side == ExprJoinSide::Right {
                                    condition.flip_sides();
                                }

                                extracted.comparisons.push(condition);
                                continue;
                            }

                            // TODO: Do we need to push to arbitrary here?
                        }
                        other => {
                            extracted.arbitrary.push(other);
                        }
                    }
                }
                ExprJoinSide::Right => {
                    if matches!(self.join_type, JoinType::Left | JoinType::Inner) {
                        // Filter right input into LEFT join.
                        extracted.right_filter.push(expr);
                    } else {
                        extracted.arbitrary.push(expr);
                    }
                }
                ExprJoinSide::Left => {
                    if matches!(self.join_type, JoinType::Right | JoinType::Inner) {
                        // Filter left input into RIGHT join.
                        extracted.left_filter.push(expr);
                    } else {
                        extracted.arbitrary.push(expr);
                    }
                }
                _ => {
                    extracted.arbitrary.push(expr);
                }
            }
        }

        Ok(extracted)
    }

    /// Try to get a comparison operator from an expression, treating the
    /// expression as a join condition between left and right.
    ///
    /// Assumes that the expression has already been split on conjunctions.
    ///
    /// Returns None if the expression is not a comparison between left and
    /// right.
    pub fn try_get_comparison_operator(
        &self,
        expr: &Expression,
    ) -> Result<Option<ComparisonOperator>> {
        if let Expression::Comparison(ComparisonExpr { left, right, op }) = expr {
            let left_side = ExprJoinSide::try_from_expr(left, self.left_tables, self.right_tables)?;
            let right_side =
                ExprJoinSide::try_from_expr(right, self.left_tables, self.right_tables)?;

            if left_side != ExprJoinSide::Both
                && right_side != ExprJoinSide::Both
                && left_side != right_side
            {
                return Ok(Some(*op));
            }
        }

        Ok(None)
    }
}
