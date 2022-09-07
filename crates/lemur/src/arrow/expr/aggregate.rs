use crate::arrow::column::Column;
use crate::arrow::expr::ScalarExpr;
use crate::arrow::scalar::ScalarOwned;
use crate::errors::{internal, Result};
use std::fmt::Debug;

pub trait AggregateExpr: Sync + Send + Debug {
    /// Return the required inputs for this aggregate expression. The results of
    /// execution for these expressions will be passed into the accumulator.
    fn inputs(&self) -> Vec<ScalarExpr>;

    /// The accumulator that this aggregate should use.
    fn accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator: Sync + Send + Debug {
    /// Update this accumulators inner state with the provided columns.
    fn accumulate(&mut self, cols: &[Column]) -> Result<()>;

    /// Evaluate the final result of accumulating.
    fn evaluate(&self) -> Result<ScalarOwned>;
}

/// A set of expressions in the group by clause of a query.
#[derive(Debug, Clone)]
pub struct GroupByExpr {
    /// Expressions for each group.
    exprs: Vec<ScalarExpr>,
    /// Null mask for each group, where a value of `true` will produce nulls.
    ///
    /// For example, for the following groups ((c1, c2), (c1), (c2)) would have
    /// the given null mask:
    ///
    /// ``` text
    /// null_mask = [
    ///   [false, false],
    ///   [false, true],
    ///   [true, false],
    /// ]
    /// ```
    null_mask: Vec<Vec<bool>>,
}

impl GroupByExpr {
    pub fn new(exprs: Vec<ScalarExpr>, null_mask: Vec<Vec<bool>>) -> Result<GroupByExpr> {
        for n in null_mask.iter() {
            if n.len() != exprs.len() {
                return Err(internal!("null mask has invalid length"));
            }
        }
        Ok(GroupByExpr { exprs, null_mask })
    }

    pub fn single_group(exprs: Vec<ScalarExpr>) -> GroupByExpr {
        let null_mask = vec![vec![false; exprs.len()]; 1];
        GroupByExpr { exprs, null_mask }
    }

    pub fn num_groups(&self) -> usize {
        self.null_mask.len()
    }

    pub fn get_exprs(&self) -> &[ScalarExpr] {
        &self.exprs
    }

    pub fn get_null_mask(&self) -> &[Vec<bool>] {
        &self.null_mask
    }
}
