use rayexec_bullet::bitmap::Bitmap;
use rayexec_error::{RayexecError, Result};

use crate::logical::operator;

/// Represents groups in the GROUP BY clause.
///
/// Examples:
///
/// Assuming a table with columns 'make', 'model', and 'sales' in that order.
///
/// ```text
/// Query: SELECT make, sum(sales) FROM items_sold GROUP BY make;
/// GroupSet {
///     columns: vec![0],
///     null_masks: vec![
///         0000,
///     ],
/// }
/// ```
///
/// ```text
/// Query: SELECT make, model, sum(sales) FROM items_sold GROUP BY make, model;
/// GroupSet {
///     columns: vec![0, 1],
///     null_masks: vec![
///         0000,
///     ],
/// }
/// ```
///
/// ```text
/// Query: SELECT make, model, sum(sales) FROM items_sold GROUP BY ROLLUP(make, model);
/// Equivalent: SELECT make, model, sum(sales) FROM items_sold GROUP BY GROUPING SETS((make, model), (make), ());
/// GroupSet {
///     columns: vec![0, 1],
///     null_masks: vec![
///         0000,
///         0001,
///         0011,
///     ],
/// }
/// ```
///
/// ```text
/// Query: SELECT make, model, sum(sales) FROM items_sold GROUP BY CUBE (make, model);
/// Equivalent: SELECT make, model, sum(sales) FROM items_sold GROUP BY GROUPING SETS((make, model), (make), (model), ());
/// GroupSet {
///     columns: vec![0, 1],
///     null_masks: vec![
///         0000,
///         0001,
///         0010,
///         0011,
///     ],
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct GroupingSets {
    /// All distinct columns used in all of the grouping sets.
    columns: Vec<usize>,

    /// Masks indicating columns that shouldn't be part of the group.
    null_masks: Vec<Bitmap>,
}

impl GroupingSets {
    pub fn try_new(columns: Vec<usize>, null_masks: Vec<Bitmap>) -> Result<Self> {
        for null_mask in &null_masks {
            if null_mask.len() != columns.len() {
                return Err(RayexecError::new(format!(
                    "Unexpected null mask size of {}, expected {}",
                    null_mask.len(),
                    columns.len()
                )));
            }
        }

        Ok(GroupingSets {
            columns,
            null_masks,
        })
    }

    pub fn num_groups(&self) -> usize {
        self.null_masks.len()
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    pub fn null_masks(&self) -> &[Bitmap] {
        &self.null_masks
    }

    /// Grouping set for a simple `GROUP BY a, b, ...`
    pub fn new_single(columns: Vec<usize>) -> Self {
        let null_masks = vec![Bitmap::from_iter(vec![false; columns.len()])];
        Self::try_new(columns, null_masks).expect("null mask to be valid")
    }

    /// Create a new grouping sets from a logical grouping expression.
    pub fn try_from_grouping_expr(expr: operator::GroupingExpr) -> Result<Self> {
        match expr {
            operator::GroupingExpr::GroupBy(cols_exprs) => {
                let cols = cols_exprs
                    .into_iter()
                    .map(|expr| expr.try_into_column_ref()?.try_as_uncorrelated())
                    .collect::<Result<Vec<_>>>()?;
                let null_masks = vec![Bitmap::new_with_val(false, cols.len())];
                GroupingSets::try_new(cols, null_masks)
            }
            operator::GroupingExpr::Rollup(cols_exprs) => {
                let cols = cols_exprs
                    .into_iter()
                    .map(|expr| expr.try_into_column_ref()?.try_as_uncorrelated())
                    .collect::<Result<Vec<_>>>()?;

                // Generate all null masks.
                //
                // E.g. for rollup on 4 columns:
                // [
                //   0000,
                //   0001,
                //   0011,
                //   0111,
                //   1111,
                // ]
                let mut null_masks = Vec::with_capacity(cols.len() + 1);
                for num_null_cols in 0..cols.len() {
                    let iter = std::iter::repeat(false)
                        .take(cols.len() - num_null_cols)
                        .chain(std::iter::repeat(true).take(num_null_cols));
                    let null_mask = Bitmap::from_iter(iter);
                    null_masks.push(null_mask);
                }

                // Append null mask with all columns marked as null (the final
                // rollup).
                null_masks.push(Bitmap::all_true(cols.len()));

                GroupingSets::try_new(cols, null_masks)
            }
            operator::GroupingExpr::Cube(_) => {
                unimplemented!("https://github.com/GlareDB/rayexec/issues/38")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use operator::LogicalExpression;

    use super::*;

    #[test]
    fn from_grouping_expr_group_by_first() {
        // t1(a, b, c)
        //
        // SELECT count(*) FROM t1 GROUP BY a;

        let expr = operator::GroupingExpr::GroupBy(vec![LogicalExpression::new_column(0)]);
        let got = GroupingSets::try_from_grouping_expr(expr).unwrap();

        let expected = GroupingSets {
            columns: vec![0],
            null_masks: vec![Bitmap::new_with_val(false, 1)],
        };

        assert_eq!(expected, got)
    }

    #[test]
    fn from_grouping_expr_group_by_second() {
        // t1(a, b, c)
        //
        // SELECT count(*) FROM t1 GROUP BY b;

        let expr = operator::GroupingExpr::GroupBy(vec![LogicalExpression::new_column(1)]);
        let got = GroupingSets::try_from_grouping_expr(expr).unwrap();

        let expected = GroupingSets {
            columns: vec![1],
            null_masks: vec![Bitmap::new_with_val(false, 1)],
        };

        assert_eq!(expected, got)
    }

    #[test]
    fn from_grouping_expr_group_by_second_third() {
        // t1(a, b, c)
        //
        // SELECT count(*) FROM t1 GROUP BY b, c;

        let expr = operator::GroupingExpr::GroupBy(vec![
            LogicalExpression::new_column(1),
            LogicalExpression::new_column(2),
        ]);
        let got = GroupingSets::try_from_grouping_expr(expr).unwrap();

        let expected = GroupingSets {
            columns: vec![1, 2],
            null_masks: vec![Bitmap::new_with_val(false, 2)],
        };

        assert_eq!(expected, got)
    }

    #[test]
    fn from_grouping_expr_group_by_third_second() {
        // t1(a, b, c)
        //
        // SELECT count(*) FROM t1 GROUP BY c, b;

        let expr = operator::GroupingExpr::GroupBy(vec![
            LogicalExpression::new_column(2),
            LogicalExpression::new_column(1),
        ]);
        let got = GroupingSets::try_from_grouping_expr(expr).unwrap();

        let expected = GroupingSets {
            columns: vec![2, 1],
            null_masks: vec![Bitmap::new_with_val(false, 2)],
        };

        assert_eq!(expected, got)
    }

    #[test]
    fn from_grouping_expr_rollup() {
        // t1(a, b, c)
        //
        // SELECT count(*) FROM t1 GROUP BY ROLLUP (a, b);

        let expr = operator::GroupingExpr::Rollup(vec![
            LogicalExpression::new_column(0),
            LogicalExpression::new_column(1),
        ]);
        let got = GroupingSets::try_from_grouping_expr(expr).unwrap();

        let expected = GroupingSets {
            columns: vec![0, 1],
            null_masks: vec![
                Bitmap::from_iter([false, false]),
                Bitmap::from_iter([false, true]),
                Bitmap::from_iter([true, true]),
            ],
        };

        assert_eq!(expected, got)
    }
}
