use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::executor::scalar::{interleave, SelectExecutor};
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::Result;

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhysicalWhenThen {
    pub when: PhysicalScalarExpression,
    pub then: PhysicalScalarExpression,
}

impl fmt::Display for PhysicalWhenThen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.when, self.then)
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCaseExpr {
    pub cases: Vec<PhysicalWhenThen>,
    pub else_expr: Box<PhysicalScalarExpression>,
}

impl PhysicalCaseExpr {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        let mut arrays = Vec::new();
        let mut indices: Vec<(usize, usize)> = (0..batch.num_rows()).map(|_| (0, 0)).collect();

        // Track remaining rows we need to evaluate.
        //
        // True bits are rows we still need to consider.
        let mut remaining = Bitmap::new_with_all_true(batch.num_rows());

        let mut trues_sel = SelectionVector::with_capacity(batch.num_rows());

        for case in &self.cases {
            // Generate selection from remaining bitmap.
            let selection = Arc::new(SelectionVector::from_iter(remaining.index_iter()));

            // Get batch with only remaining rows that we should consider.
            let selected_batch = batch.select(selection.clone());

            // Execute 'when'.
            let selected = case.when.eval(&selected_batch)?;

            // Determine which rows should be executed for 'then', and which we
            // need to fall through on.
            SelectExecutor::select(&selected, &mut trues_sel)?;

            if trues_sel.num_rows() == 0 {
                // No rows selected, move to next case.
                continue;
            }

            // Select rows in batch to execute on based on 'trues'.
            let execute_batch = selected_batch.select(Arc::new(trues_sel.clone()));
            let output = case.then.eval(&execute_batch)?;

            // Store array for later interleaving.
            let array_idx = arrays.len();
            arrays.push(output.into_owned());

            // Figure out mapping from the 'trues' selection to the original row
            // index.
            //
            // The selection vector locations should index into the full-length
            // selection vector to get the original row index.
            for (array_row_idx, selected_row_idx) in trues_sel.iter_locations().enumerate() {
                // Final output row.
                let output_row_idx = selection.get(selected_row_idx);
                indices[output_row_idx] = (array_idx, array_row_idx);

                // Update bitmap, this row was handled.
                remaining.set_unchecked(output_row_idx, false);
            }
        }

        // Do all remaining rows.
        if remaining.count_trues() != 0 {
            let selection = Arc::new(SelectionVector::from_iter(remaining.index_iter()));
            let remaining_batch = batch.select(selection.clone());

            let output = self.else_expr.eval(&remaining_batch)?;
            let array_idx = arrays.len();
            arrays.push(output.into_owned());

            // Update indices.
            for (array_row_idx, output_row_idx) in selection.iter_locations().enumerate() {
                indices[output_row_idx] = (array_idx, array_row_idx);
            }
        }

        // Interleave.
        let refs: Vec<_> = arrays.iter().collect();
        let arr = interleave(&refs, &indices)?;

        Ok(Cow::Owned(arr))
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

#[cfg(test)]
mod tests {

    use rayexec_bullet::scalar::ScalarValue;

    use super::*;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
    use crate::expr::physical::scalar_function_expr::PhysicalScalarFunctionExpr;
    use crate::functions::scalar::comparison::EqImpl;

    #[test]
    fn case_simple() {
        let batch = Batch::try_new([
            Array::from_iter([1, 2, 3, 4]),
            Array::from_iter([12, 13, 14, 15]),
        ])
        .unwrap();

        // CASE WHEN a = 2 THEN 'first_case'
        //      WHEN a = 3 THEN 'second_case'
        //      ELSE 'else'
        // END
        let case = PhysicalCaseExpr {
            cases: vec![
                PhysicalWhenThen {
                    when: PhysicalScalarExpression::ScalarFunction(PhysicalScalarFunctionExpr {
                        function: Box::new(EqImpl),
                        inputs: vec![
                            PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 0 }),
                            PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                                literal: ScalarValue::from(2),
                            }),
                        ],
                    }),
                    then: PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                        literal: ScalarValue::from("first_case"),
                    }),
                },
                PhysicalWhenThen {
                    when: PhysicalScalarExpression::ScalarFunction(PhysicalScalarFunctionExpr {
                        function: Box::new(EqImpl),
                        inputs: vec![
                            PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 0 }),
                            PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                                literal: ScalarValue::from(3),
                            }),
                        ],
                    }),
                    then: PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                        literal: ScalarValue::from("second_case"),
                    }),
                },
            ],
            else_expr: Box::new(PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                literal: ScalarValue::from("else"),
            })),
        };

        let got = case.eval(&batch).unwrap();

        assert_eq!(ScalarValue::from("else"), got.logical_value(0).unwrap());
        assert_eq!(
            ScalarValue::from("first_case"),
            got.logical_value(1).unwrap()
        );
        assert_eq!(
            ScalarValue::from("second_case"),
            got.logical_value(2).unwrap()
        );
        assert_eq!(ScalarValue::from("else"), got.logical_value(3).unwrap());
    }
}
