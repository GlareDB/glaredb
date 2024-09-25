use std::{fmt, sync::Arc};

use rayexec_bullet::{array::Array, batch::Batch, bitmap::Bitmap, compute};
use rayexec_error::Result;

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhyscialWhenThen {
    pub when: PhysicalScalarExpression,
    pub then: PhysicalScalarExpression,
}

impl fmt::Display for PhyscialWhenThen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.when, self.then)
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCaseExpr {
    pub cases: Vec<PhyscialWhenThen>,
    pub else_expr: Box<PhysicalScalarExpression>,
}

impl PhysicalCaseExpr {
    pub fn eval(&self, batch: &Batch, selection: Option<&Bitmap>) -> Result<Arc<Array>> {
        let mut interleave_indices: Vec<_> = (0..batch.num_rows()).map(|_| (0, 0)).collect();

        let mut case_outputs = Vec::new();

        // Determines which rows we need to compute results for. If we already
        // have a selection, use that, otherwise we'll be looking at all rows.
        let mut needs_results = match selection {
            Some(selection) => selection.clone(),
            None => Bitmap::all_true(batch.num_rows()),
        };

        for case in &self.cases {
            // No need to evaluate any more cases.
            if needs_results.count_trues() == 0 {
                break;
            }

            let mut selected_rows = case.when.select(batch, Some(&needs_results))?;
            // No cases returned true.
            if selected_rows.count_trues() == 0 {
                continue;
            }

            // Update when bitmap to evaluate only the rows we haven't computed
            // results for yet.
            selected_rows.bit_and_mut(&needs_results)?;

            let then_result = case.then.eval(batch, Some(&selected_rows))?;

            // Update bitmap to skip these rows in the next case.
            needs_results.bit_and_not_mut(&selected_rows)?;

            // Store array, update interleave indices to point to these rows.
            let arr_idx = case_outputs.len();
            case_outputs.push(then_result);

            for (arr_row_idx, final_row_idx) in selected_rows.index_iter().enumerate() {
                interleave_indices[final_row_idx] = (arr_idx, arr_row_idx);
            }
        }

        // Evaluate any remaining rows.
        if needs_results.count_trues() != 0 {
            let else_result = self.else_expr.eval(batch, Some(&needs_results))?;

            let arr_idx = case_outputs.len();
            case_outputs.push(else_result);

            for (arr_row_idx, final_row_idx) in needs_results.index_iter().enumerate() {
                interleave_indices[final_row_idx] = (arr_idx, arr_row_idx);
            }
        }

        // All rows accounted for, compute 'interleave' indices for building the
        // final batch.

        let arrs: Vec<_> = case_outputs.iter().map(|arr| arr.as_ref()).collect();
        let out = compute::interleave::interleave(&arrs, &interleave_indices)?;

        Ok(Arc::new(out))
    }
}

impl fmt::Display for PhysicalCaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE ")?;
        for case in &self.cases {
            write!(f, "{} ", case)?;
        }
        write!(f, "ELSE {}", self.else_expr)?;

        Ok(())
    }
}
