use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use rayexec_error::Result;

use super::PhysicalScalarExpression;
use crate::arrays::array::Array2;
use crate::arrays::batch::Batch;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::executor::scalar::{interleave, SelectExecutor};
use crate::arrays::selection::SelectionVector;

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
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array2>> {
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

    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::scalar::ScalarValue;
    use crate::expr::case_expr::{CaseExpr, WhenThen};
    use crate::expr::physical::planner::PhysicalExpressionPlanner;
    use crate::expr::{self, Expression};
    use crate::functions::scalar::builtin::comparison::Eq;
    use crate::functions::scalar::ScalarFunction;
    use crate::logical::binder::table_list::TableList;

    #[test]
    fn case_simple() {
        let batch = Batch::try_new([
            Array2::from_iter([1, 2, 3, 4]),
            Array2::from_iter([12, 13, 14, 15]),
        ])
        .unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        // CASE WHEN a = 2 THEN 'first_case'
        //      WHEN a = 3 THEN 'second_case'
        //      ELSE 'else'
        // END

        let when_expr_0 = Expression::ScalarFunction(
            Eq.plan(&table_list, vec![expr::col_ref(table_ref, 0), expr::lit(2)])
                .unwrap()
                .into(),
        );
        let then_expr_0 = expr::lit("first_case");

        let when_expr_1 = Expression::ScalarFunction(
            Eq.plan(&table_list, vec![expr::col_ref(table_ref, 0), expr::lit(3)])
                .unwrap()
                .into(),
        );
        let then_expr_1 = expr::lit("second_case");

        let else_expr = expr::lit("else");

        let case_expr = Expression::Case(CaseExpr {
            cases: vec![
                WhenThen {
                    when: when_expr_0,
                    then: then_expr_0,
                },
                WhenThen {
                    when: when_expr_1,
                    then: then_expr_1,
                },
            ],
            else_expr: Some(Box::new(else_expr)),
        });

        let planner = PhysicalExpressionPlanner::new(&table_list);
        let physical_case = planner.plan_scalar(&[table_ref], &case_expr).unwrap();

        let got = physical_case.eval(&batch).unwrap();

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
