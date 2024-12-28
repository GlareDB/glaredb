use std::fmt;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array2;
use crate::arrays::batch::Batch2;
use crate::arrays::executor::scalar::SelectExecutor;
use crate::arrays::selection::SelectionVector;
use crate::expr::physical::PhysicalScalarExpression;
use crate::functions::scalar::builtin::boolean::AndImpl;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunctionImpl};

#[derive(Debug, Clone)]
pub struct HashJoinCondition {
    /// The left expression.
    pub left: PhysicalScalarExpression,
    /// The right expression.
    pub right: PhysicalScalarExpression,
    /// The function to use for the comparison.
    ///
    /// This should be planned function for the comparison operator this
    /// condition was created for. Assumed to take exactly two inputs (left and
    /// right).
    pub function: PlannedScalarFunction,
}

impl fmt::Display for HashJoinCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(LEFT {}) {} (RIGHT {})",
            self.left,
            self.function.function.name(),
            self.right
        )
    }
}

/// Join condition with the left side precomputed.
///
/// When inserting into the hash table for the left side, the left scalar
/// expression should be executed on the batch with the result being pushed to
/// `lefts`.
#[derive(Debug)]
pub struct LeftPrecomputedJoinCondition {
    /// Precomputed results for left batches.
    pub left_precomputed: Vec<Array2>,
    pub left: PhysicalScalarExpression,
    pub right: PhysicalScalarExpression,
    pub function: PlannedScalarFunction,
}

impl LeftPrecomputedJoinCondition {
    pub fn from_condition_with_capacity(condition: HashJoinCondition, cap: usize) -> Self {
        LeftPrecomputedJoinCondition {
            left_precomputed: Vec::with_capacity(cap),
            left: condition.left,
            right: condition.right,
            function: condition.function,
        }
    }
}

impl From<HashJoinCondition> for LeftPrecomputedJoinCondition {
    fn from(value: HashJoinCondition) -> Self {
        Self::from_condition_with_capacity(value, 0)
    }
}

/// All conditions for a single join.
#[derive(Debug)]
pub struct LeftPrecomputedJoinConditions {
    pub conditions: Vec<LeftPrecomputedJoinCondition>,
}

impl LeftPrecomputedJoinConditions {
    /// Compute the left side of the condition using the provided batch as
    /// input.
    pub fn precompute_for_left_batch(&mut self, left: &Batch2) -> Result<()> {
        for condition in &mut self.conditions {
            let precomputed = condition.left.eval(left)?;
            condition.left_precomputed.push(precomputed.into_owned())
        }

        Ok(())
    }

    /// Compute the output selection array by ANDing the results of all
    /// conditions.
    ///
    /// The output is the (left, right) selection vectors to use for the final
    /// output batch.
    pub fn compute_selection_for_probe(
        &self,
        left_batch_idx: usize,
        left_row_sel: SelectionVector,
        right_row_sel: SelectionVector,
        right: &Batch2,
    ) -> Result<(SelectionVector, SelectionVector)> {
        assert_eq!(left_row_sel.num_rows(), right_row_sel.num_rows());

        let left_row_sel = Arc::new(left_row_sel);
        let right_row_sel = Arc::new(right_row_sel);

        let mut results = Vec::with_capacity(self.conditions.len());

        // Select rows from the right batch.
        let selected_right = right.select(right_row_sel.clone());

        for condition in &self.conditions {
            let mut left_precomputed = condition
                .left_precomputed
                .get(left_batch_idx)
                .ok_or_else(|| {
                    RayexecError::new(format!("Missing left precomputed array: {left_batch_idx}"))
                })?
                .clone();

            // Select relevant rows from the left.
            left_precomputed.select_mut(left_row_sel.clone());

            // Eval the right side.
            let right_arr = condition.right.eval(&selected_right)?;

            // Compute join condition result.
            let result = condition
                .function
                .function_impl
                .execute(&[&left_precomputed, right_arr.as_ref()])?;

            results.push(result);
        }

        // AND the results.
        let refs: Vec<_> = results.iter().collect();
        let out = AndImpl.execute(&refs)?;

        // Generate a selection for the left and right selections.
        let mut select_the_selection = SelectionVector::with_capacity(out.logical_len());
        SelectExecutor::select(&out, &mut select_the_selection)?;

        // Filter the original selection vectors only keeping selected indices.
        let left_row_sel = left_row_sel.select(&select_the_selection);
        let right_row_sel = right_row_sel.select(&select_the_selection);

        Ok((left_row_sel, right_row_sel))
    }
}
