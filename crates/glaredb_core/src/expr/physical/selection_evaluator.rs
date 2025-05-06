use glaredb_error::Result;

use super::PhysicalScalarExpression;
use super::evaluator::ExpressionEvaluator;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::conjunction_expr::ConjunctionOperator;

/// Wrapper around an expression evaluator for computing selections on batches.
#[derive(Debug)]
pub struct SelectionEvaluator {
    /// Reusable selection buffer.
    pub(crate) selection: Vec<usize>,
    /// Secondary selection buffer.
    ///
    /// Used during short-circuit evaluation.
    pub(crate) selection_sec: Vec<usize>,
    /// Batch for writing intermediate boolean outputs to.
    ///
    /// Should contain a single boolean array.
    pub(crate) output: Batch,
    /// The underlying evaluator.
    pub(crate) evaluator: ExpressionEvaluator,
}

impl SelectionEvaluator {
    pub fn try_new(
        expression: impl Into<PhysicalScalarExpression>,
        batch_size: usize,
    ) -> Result<Self> {
        let evaluator = ExpressionEvaluator::try_new([expression.into()], batch_size)?;
        let output = Batch::new([DataType::boolean()], batch_size)?;
        let selection = Vec::with_capacity(batch_size);
        let selection_sec = Vec::with_capacity(batch_size);

        Ok(SelectionEvaluator {
            selection,
            selection_sec,
            output,
            evaluator,
        })
    }

    pub fn selection(&self) -> &[usize] {
        &self.selection
    }

    /// Select rows from the input based on the expression evaluating to 'true'
    ///
    /// Expressions that evaluate to false or NULL will not be part of the
    /// selection.
    ///
    /// The internal state is cleared across calls to this method.
    pub fn select(&mut self, input: &mut Batch) -> Result<&[usize]> {
        debug_assert_eq!(1, self.evaluator.num_expressions());
        debug_assert_eq!(1, self.output.arrays.len());
        debug_assert_eq!(&DataType::boolean(), self.output.arrays[0].datatype());

        let (exprs, states) = self.evaluator.eval_parts_mut();

        match &exprs[0] {
            PhysicalScalarExpression::Conjunction(conj) if conj.op == ConjunctionOperator::And => {
                // AND short circuiting.
                self.selection.clear();
                self.selection_sec.clear();

                let state = &mut states[0];

                // Initial selection is all rows.
                self.selection.extend(input.selection());

                for (expr_idx, expr) in conj.expr.inputs.iter().enumerate() {
                    self.output.reset_for_write()?;

                    let child_state = &mut state.inputs[expr_idx];
                    child_state.reset_for_write()?;

                    // Eval just this child expression.
                    ExpressionEvaluator::eval_expression(
                        expr,
                        input,
                        child_state,
                        Selection::slice(&self.selection),
                        &mut self.output.arrays[0],
                    )?;

                    self.selection_sec.clear();
                    UnaryExecutor::select(
                        &self.output.arrays[0],
                        0..self.selection.len(),
                        |local_idx| {
                            // This row evaluated to true. Push the original row
                            // index to the secondary selection.
                            let row = self.selection[local_idx];
                            self.selection_sec.push(row);
                        },
                        |_| {
                            // Row evaluated to false. Don't evaluate the rest
                            // of the expressions, just drop...
                        },
                    )?;

                    // Swap selection arrays such that `selection` always
                    // contains the real row index.
                    std::mem::swap(&mut self.selection, &mut self.selection_sec);

                    if self.selection.is_empty() {
                        // Nothing left, stop early.
                        break;
                    }
                }

                Ok(&self.selection)
            }
            _ => {
                // Everything else, just evaluate all the expressions.
                self.selection.clear();
                self.output.reset_for_write()?;

                self.evaluator
                    .eval_batch(input, input.selection(), &mut self.output)?;

                // Provide selection relative to the boolean output array.
                UnaryExecutor::select(
                    &self.output.arrays[0],
                    0..input.num_rows(),
                    |idx| self.selection.push(idx),
                    |_| {}, // Do nothing for the false case.
                )?;

                Ok(&self.selection)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::functions::scalar::builtin::debug::FUNCTION_SET_DEBUG_ERROR_ON_EXECUTE;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::exprs::plan_scalar;
    use crate::{expr, generate_batch};

    #[test]
    fn select_simple() {
        let mut evaluator =
            SelectionEvaluator::try_new(PhysicalColumnExpr::new(0, DataType::boolean()), 1024)
                .unwrap();

        let mut input = generate_batch!([true, false, true, true], [8, 9, 7, 6]);

        let selection = evaluator.select(&mut input).unwrap();
        assert_eq!(&[0, 2, 3], selection);

        // Make sure we reset internal state.
        let mut input = generate_batch!([true, false, false, false], [2, 2, 2, 2]);

        let selection = evaluator.select(&mut input).unwrap();
        assert_eq!(&[0], selection);
    }

    #[test]
    fn select_with_null() {
        let mut evaluator =
            SelectionEvaluator::try_new(PhysicalColumnExpr::new(0, DataType::boolean()), 1024)
                .unwrap();

        let mut input = generate_batch!([Some(true), Some(false), None, Some(true)], [8, 9, 7, 6]);

        let selection = evaluator.select(&mut input).unwrap();
        assert_eq!(&[0, 3], selection);
    }

    #[test]
    fn select_and_short_circuit() {
        let mut list = TableList::empty();
        let t0 = list
            .push_table(None, [DataType::boolean(), DataType::int32()], ["c1", "c2"])
            .unwrap();

        // c1 = true AND c2 > 4
        let expr = plan_scalar(
            &list,
            expr::and([
                expr::eq(expr::column((t0, 0), DataType::boolean()), expr::lit(true))
                    .unwrap()
                    .into(),
                expr::gt(expr::column((t0, 1), DataType::int32()), expr::lit(4))
                    .unwrap()
                    .into(),
            ])
            .unwrap(),
        );

        let mut evaluator = SelectionEvaluator::try_new(expr, 16).unwrap();
        let mut input = generate_batch!([true, false, true, true, false, true], [5, 5, 1, 6, 2, 8]);

        let selection = evaluator.select(&mut input).unwrap();

        let expected = [0, 3, 5];
        assert_eq!(&expected, selection);
    }

    #[test]
    fn select_and_short_circuit_no_eval_last() {
        let mut list = TableList::empty();
        let t0 = list
            .push_table(None, [DataType::boolean(), DataType::int32()], ["c1", "c2"])
            .unwrap();

        // c1 = true AND c2 > 4 AND debug_error_on_execute() = 3
        let expr = plan_scalar(
            &list,
            expr::and([
                expr::eq(expr::column((t0, 0), DataType::boolean()), expr::lit(true))
                    .unwrap()
                    .into(),
                expr::gt(expr::column((t0, 1), DataType::int32()), expr::lit(4))
                    .unwrap()
                    .into(),
                expr::eq(
                    expr::scalar_function(&FUNCTION_SET_DEBUG_ERROR_ON_EXECUTE, Vec::new())
                        .unwrap(),
                    expr::lit(3),
                )
                .unwrap()
                .into(),
            ])
            .unwrap(),
        );

        let mut evaluator = SelectionEvaluator::try_new(expr, 16).unwrap();
        // All inputs must eval to false before reaching the last expression
        // (the erroring function).
        let mut input = generate_batch!([true, false, true, true, false, true], [4, 5, 1, 2, 2, 1]);

        let selection = evaluator.select(&mut input).unwrap();

        assert!(selection.is_empty());
    }
}
