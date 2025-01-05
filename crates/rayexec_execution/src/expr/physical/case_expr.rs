use std::fmt;

use rayexec_error::Result;

use super::{ExpressionState, PhysicalScalarExpression};
use crate::arrays::array::exp::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::buffer_manager::NopBufferManager;
use crate::arrays::buffer::physical_type::PhysicalBool;
use crate::arrays::datatype::DataType;
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;
use crate::expr::physical::evaluator::ExpressionEvaluator;

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

/// Physical expression for 'CASE .. THEN .. ELSE ..' expressions.
#[derive(Debug, Clone)]
pub struct PhysicalCaseExpr {
    pub cases: Vec<PhysicalWhenThen>,
    pub else_expr: Box<PhysicalScalarExpression>,
    pub datatype: DataType,
}

impl PhysicalCaseExpr {
    pub(crate) fn create_state(&self, batch_size: usize) -> Result<ExpressionState> {
        // 2 states per when/then pair, plus one for the 'else'.
        let mut inputs = Vec::with_capacity(self.cases.len() * 2 + 1);
        for case in &self.cases {
            let when_input = case.when.create_state(batch_size)?;
            inputs.push(when_input);

            let then_input = case.then.create_state(batch_size)?;
            inputs.push(then_input);
        }

        let else_input = self.else_expr.create_state(batch_size)?;
        inputs.push(else_input);

        // 2 arrays in the buffer, one 'boolean' for conditional evaluation, one
        // for the result if condition is true. 'then' and 'else' expressions
        // should evaluate to the same type.
        let buffer = Batch::from_arrays(
            [
                Array::new(&NopBufferManager, DataType::Boolean, batch_size)?,
                Array::new(&NopBufferManager, self.else_expr.datatype(), batch_size)?,
            ],
            false,
        )?;

        Ok(ExpressionState { buffer, inputs })
    }

    pub fn datatype(&self) -> DataType {
        self.datatype.clone()
    }

    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        // Indices where 'when' evaluated to true and the 'then' expression
        // needs to be evaluated.
        let mut then_selection = Vec::with_capacity(sel.len());
        // Indices where 'then' evaluated to false or null.
        let mut fallthrough_selection = Vec::with_capacity(sel.len());

        // Current selection for a single when/then pair.
        //
        // Initialized to the initial selection passed in.
        let mut curr_selection: Vec<_> = sel.iter().collect(); // TODO: Would be cool not needing to allocate here.

        for (case_idx, case) in self.cases.iter().enumerate() {
            fallthrough_selection.clear();
            then_selection.clear();

            if curr_selection.is_empty() {
                // Nothing left to do.
                break;
            }

            // Each case has two input states, one for 'when' and one for
            // 'then'.
            let when_state = &mut state.inputs[case_idx * 2];
            // When array reused for each case.
            let when_array = &mut state.buffer.arrays_mut()[0];
            when_array.reset_for_write(&NopBufferManager)?;

            // Eval 'when'
            ExpressionEvaluator::eval_expression(
                &case.when,
                input,
                when_state,
                Selection::selection(&curr_selection),
                when_array,
            )?;

            UnaryExecutor::for_each_flat::<PhysicalBool, _>(
                when_array.flat_view()?,
                Selection::selection(&curr_selection),
                |idx, b| {
                    if let Some(&true) = b {
                        // 'When' expression evaluated to true, select it for
                        // 'then' expression eval.
                        then_selection.push(idx);
                    } else {
                        // Not true, need to fall through.
                        fallthrough_selection.push(idx);
                    }
                },
            )?;

            if then_selection.is_empty() {
                // Everything in this case's 'when' evaluated to false.
                continue;
            }

            let then_state = &mut state.inputs[case_idx * 2 + 1];
            // Reused, assumes all 'then' expressions and the 'else' expression
            // are the same type.
            let then_array = &mut state.buffer.arrays_mut()[1];
            then_array.reset_for_write(&NopBufferManager)?;

            // Eval 'then' with selection from 'when'.
            ExpressionEvaluator::eval_expression(
                &case.then,
                input,
                then_state,
                Selection::selection(&then_selection),
                then_array,
            )?;

            // Fill output array according to indices in 'when' selection.
            then_array.copy_rows(then_selection.iter().copied().enumerate(), output)?;

            // Update next iteration to use fallthrough indices.
            std::mem::swap(&mut fallthrough_selection, &mut curr_selection);
        }

        if !curr_selection.is_empty() {
            // We have remaining indices that fell through all cases. Eval with
            // else expression and add those in.
            let else_state = state.inputs.last_mut().unwrap(); // Last state after all when/then states.
            let else_array = &mut state.buffer.arrays_mut()[1];
            else_array.reset_for_write(&NopBufferManager)?;

            ExpressionEvaluator::eval_expression(
                &self.else_expr,
                input,
                else_state,
                Selection::selection(&curr_selection),
                else_array,
            )?;

            // And fill remaining.
            else_array.copy_rows(curr_selection.iter().copied().enumerate(), output)?;
        }

        Ok(())
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
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;

    #[test]
    fn case_simple() {
        // CASE a THEN b
        // ELSE 48
        let expr = PhysicalCaseExpr {
            cases: vec![PhysicalWhenThen {
                when: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                    idx: 0,
                    datatype: DataType::Boolean,
                }),
                then: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                    idx: 1,
                    datatype: DataType::Int32,
                }),
            }],
            else_expr: Box::new(PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                literal: 48.into(),
            })),
            datatype: DataType::Int32,
        };

        let mut input = Batch::from_arrays(
            [
                Array::try_from_iter([true, true, false]).unwrap(),
                Array::try_from_iter([1, 2, 3]).unwrap(),
            ],
            true,
        )
        .unwrap();

        let mut state = expr.create_state(3).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(3), &mut out)
            .unwrap();

        let expected = Array::try_from_iter([1, 2, 48]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn case_falsey() {
        // Same as above but check that 'when' treats nulls as false.

        // CASE a THEN b
        // ELSE 48
        let expr = PhysicalCaseExpr {
            cases: vec![PhysicalWhenThen {
                when: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                    idx: 0,
                    datatype: DataType::Boolean,
                }),
                then: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                    idx: 1,
                    datatype: DataType::Int32,
                }),
            }],
            else_expr: Box::new(PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                literal: 48.into(),
            })),
            datatype: DataType::Int32,
        };

        let mut input = Batch::from_arrays(
            [
                Array::try_from_iter([Some(true), None, Some(false)]).unwrap(),
                Array::try_from_iter([1, 2, 3]).unwrap(),
            ],
            true,
        )
        .unwrap();

        let mut state = ExpressionState {
            buffer: Batch::new(&NopBufferManager, [DataType::Boolean, DataType::Int32], 3).unwrap(),
            inputs: vec![ExpressionState::empty(), ExpressionState::empty()],
        };

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(3), &mut out)
            .unwrap();

        let expected = Array::try_from_iter([1, 48, 48]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
