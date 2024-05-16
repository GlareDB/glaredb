use rayexec_bullet::bitmap::Bitmap;
use rayexec_error::{RayexecError, Result};

use crate::planner::operator::GroupingExpr;

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
#[derive(Debug)]
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
}
