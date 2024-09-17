use crate::{
    expr::{comparison_expr::ComparisonExpr, Expression},
    logical::{
        binder::bind_context::TableRef,
        logical_join::{ComparisonCondition, JoinType},
    },
    optimizer::filter_pushdown::split::split_conjunction,
};
use rayexec_error::{not_implemented, RayexecError, Result};

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

    pub fn try_from_table_refs<'a>(
        refs: impl IntoIterator<Item = &'a TableRef>,
        left_tables: &[TableRef],
        right_tables: &[TableRef],
    ) -> Result<ExprJoinSide> {
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
    pub fn try_from_expr(
        expr: &Expression,
        left_tables: &[TableRef],
        right_tables: &[TableRef],
    ) -> Result<ExprJoinSide> {
        fn inner(
            expr: &Expression,
            left_tables: &[TableRef],
            right_tables: &[TableRef],
            side: ExprJoinSide,
        ) -> Result<ExprJoinSide> {
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

    fn try_from_table_ref(
        table_ref: TableRef,
        left_tables: &[TableRef],
        right_tables: &[TableRef],
    ) -> Result<ExprJoinSide> {
        if left_tables.contains(&table_ref) {
            Ok(ExprJoinSide::Left)
        } else if right_tables.contains(&table_ref) {
            Ok(ExprJoinSide::Right)
        } else {
            Err(RayexecError::new(format!(
                "Table ref is invalid. Left: {left_tables:?}, right: {right_tables:?}, got: {table_ref:?}"
            )))
        }
    }
}

#[derive(Debug)]
pub struct JoinConditionExtractor<'a> {
    pub left_tables: &'a [TableRef],
    pub right_tables: &'a [TableRef],
    pub join_type: JoinType,
}

impl<'a> JoinConditionExtractor<'a> {
    pub fn new(
        left_tables: &'a [TableRef],
        right_tables: &'a [TableRef],
        join_type: JoinType,
    ) -> Self {
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
                        }
                        other => {
                            extracted.arbitrary.push(other);
                        }
                    }
                }
                ExprJoinSide::Right => {
                    if self.join_type == JoinType::Left {
                        // Filter right input into LEFT join.
                        extracted.left_filter.push(expr);
                    } else {
                        extracted.arbitrary.push(expr);
                    }
                }
                ExprJoinSide::Left => {
                    if self.join_type == JoinType::Right {
                        // Filter left input into RIGHT join.
                        extracted.right_filter.push(expr);
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
}
