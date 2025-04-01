use std::fmt;

use glaredb_error::Result;

use super::PhysicalScalarExpression;
use super::evaluator::{ExpressionEvaluator, ExpressionState};
use crate::arrays::array::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::scalar::UnaryExecutor;

#[derive(Debug, Clone)]
pub struct PhysicalWhenThen {
    pub when: PhysicalScalarExpression,
    pub then: PhysicalScalarExpression,
}

impl PhysicalWhenThen {
    pub fn new(
        when: impl Into<PhysicalScalarExpression>,
        then: impl Into<PhysicalScalarExpression>,
    ) -> Self {
        PhysicalWhenThen {
            when: when.into(),
            then: then.into(),
        }
    }
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

        // 3 arrays in the buffer, one 'boolean' for conditional evaluation, one
        // for the result if condition is true, and one for the 'else'. 'then'
        // and 'else' expressions should evaluate to the same type.
        let buffer = Batch::new(
            [
                DataType::Boolean,
                self.else_expr.datatype(),
                self.else_expr.datatype(),
            ],
            batch_size,
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
            // Sub expressions may produce constant value arrays, always ensure
            // the arrays are writable for each case.
            state.reset_for_write()?;

            then_selection.clear();
            fallthrough_selection.clear();

            if curr_selection.is_empty() {
                // Nothing left to do.
                break;
            }

            // Each case has two input states, one for 'when' and one for
            // 'then'.
            let when_state = &mut state.inputs[case_idx * 2];
            // When array reused for each case.
            let when_array = &mut state.buffer.arrays_mut()[0];

            // Eval 'when'
            ExpressionEvaluator::eval_expression(
                &case.when,
                input,
                when_state,
                Selection::slice(&curr_selection),
                when_array,
            )?;

            UnaryExecutor::select(
                when_array,
                0..curr_selection.len(),
                |idx| then_selection.push(curr_selection[idx]), // WHEN evaled to true
                |idx| fallthrough_selection.push(curr_selection[idx]), // WHEN evaled to false
            )?;

            if then_selection.is_empty() {
                // Everything in this case's 'when' evaluated to false.
                continue;
            }

            let then_state = &mut state.inputs[case_idx * 2 + 1];
            // Reused, assumes all 'then' expressions and the 'else' expression
            // are the same type.
            let then_array = &mut state.buffer.arrays_mut()[1];

            // Eval 'then' with selection from 'when'.
            ExpressionEvaluator::eval_expression(
                &case.then,
                input,
                then_state,
                Selection::slice(&then_selection),
                then_array,
            )?;

            // Fill output array according to indices in 'then' selection.
            then_array.copy_rows(then_selection.iter().copied().enumerate(), output)?;

            // Update next iteration to use fallthrough indices.
            std::mem::swap(&mut fallthrough_selection, &mut curr_selection);
        }

        if !curr_selection.is_empty() {
            // We have remaining indices that fell through all cases. Eval with
            // else expression and add those in.
            let else_state = state.inputs.last_mut().unwrap(); // Last state after all when/then states.
            let else_array = &mut state.buffer.arrays_mut()[2];

            ExpressionEvaluator::eval_expression(
                &self.else_expr,
                input,
                else_state,
                Selection::slice(&curr_selection),
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
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::scalar::ScalarValue;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
    use crate::functions::scalar::builtin::is::FUNCTION_SET_IS_NULL;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::testutil::exprs::plan_scalar;
    use crate::{expr, generate_array, generate_batch};

    fn typed_null(datatype: DataType) -> PhysicalScalarExpression {
        plan_scalar(
            &TableList::empty(),
            expr::cast(expr::lit(ScalarValue::Null), datatype).unwrap(),
        )
    }

    #[test]
    fn case_simple() {
        // CASE a THEN b
        // ELSE 48
        let cases = vec![PhysicalWhenThen::new(
            PhysicalColumnExpr::from((0, DataType::Boolean)),
            PhysicalColumnExpr::from((1, DataType::Int32)),
        )];
        let expr = PhysicalCaseExpr {
            cases,
            else_expr: Box::new(PhysicalLiteralExpr::new(48).into()),
            datatype: DataType::Int32,
        };

        let mut input = generate_batch!([true, true, false], [1, 2, 3]);

        let mut state = expr.create_state(3).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(0, 3), &mut out)
            .unwrap();

        let expected = generate_array!([1, 2, 48]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn case_falsey() {
        // Same as above but check that 'when' treats nulls as false.
        //
        // CASE a THEN b
        // ELSE 48
        let cases = vec![PhysicalWhenThen::new(
            PhysicalColumnExpr::from((0, DataType::Boolean)),
            PhysicalColumnExpr::from((1, DataType::Int32)),
        )];
        let expr = PhysicalCaseExpr {
            cases,
            else_expr: Box::new(PhysicalLiteralExpr::new(48).into()),
            datatype: DataType::Int32,
        };

        let mut input = generate_batch!([Some(true), None, Some(false)], [1, 2, 3]);

        let mut state = expr.create_state(3).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(0, 3), &mut out)
            .unwrap();

        let expected = generate_array!([1, 48, 48]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn case_multiple_whens() {
        // CASE WHEN i = 6 THEN i + 10
        //      WHEN i >= 5 THEN i + 20
        // END
        let mut table_list = TableList::empty();
        let t0 = table_list
            .push_table(None, [DataType::Int32], ["i"])
            .unwrap();

        // i = 6
        let when_0 = plan_scalar(
            &table_list,
            expr::eq(expr::column((t0, 0), DataType::Int32), expr::lit(6)).unwrap(),
        );
        // i+10
        let then_0 = plan_scalar(
            &table_list,
            expr::add(expr::column((t0, 0), DataType::Int32), expr::lit(10)).unwrap(),
        );

        // i >= 5
        let when_1 = plan_scalar(
            &table_list,
            expr::gt_eq(expr::column((t0, 0), DataType::Int32), expr::lit(5)).unwrap(),
        );
        // i + 20
        let then_1 = plan_scalar(
            &table_list,
            expr::add(expr::column((t0, 0), DataType::Int32), expr::lit(20)).unwrap(),
        );

        let cases = vec![
            PhysicalWhenThen::new(when_0, then_0),
            PhysicalWhenThen::new(when_1, then_1),
        ];

        let expr = PhysicalCaseExpr {
            cases,
            else_expr: Box::new(typed_null(DataType::Int32)),
            datatype: DataType::Int32,
        };

        let mut input = generate_batch!([Some(4), Some(5), Some(6), None]);

        let mut state = expr.create_state(4).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 4).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(0, 4), &mut out)
            .unwrap();

        let expected = generate_array!([None, Some(25), Some(16), None]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn case_with_constant_then() {
        // CASE i
        //     WHEN 4 THEN 'a'
        //     WHEN 5 THEN 'b'
        // END

        let mut table_list = TableList::empty();
        let t0 = table_list
            .push_table(None, [DataType::Int32], ["i"])
            .unwrap();

        // 'WHEN 4' gets desugared to 'WHEN i = 4' during logical planning.

        // i = 4
        let when_0 = plan_scalar(
            &table_list,
            expr::eq(expr::column((t0, 0), DataType::Int32), expr::lit(4)).unwrap(),
        );
        // 'a'
        let then_0 = plan_scalar(&table_list, expr::lit("a"));

        // i = 5
        let when_1 = plan_scalar(
            &table_list,
            expr::eq(expr::column((t0, 0), DataType::Int32), expr::lit(5)).unwrap(),
        );
        // 'b'
        let then_1 = plan_scalar(&table_list, expr::lit("b"));

        let cases = vec![
            PhysicalWhenThen::new(when_0, then_0),
            PhysicalWhenThen::new(when_1, then_1),
        ];

        let expr = PhysicalCaseExpr {
            cases,
            else_expr: Box::new(typed_null(DataType::Utf8)),
            datatype: DataType::Utf8,
        };

        let mut input = generate_batch!([Some(4), Some(5), Some(6), None]);
        let mut state = expr.create_state(4).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 4).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(0, 4), &mut out)
            .unwrap();

        let expected = generate_array!([Some("a"), Some("b"), None, None]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn case_shortcircuit_then_expr() {
        // CASE i
        //     WHEN false THEN 1 / 0
        //     ELSE 'hello'
        // END

        let table_list = TableList::empty();
        let invalid_expr = plan_scalar(&table_list, expr::div(expr::lit(1), expr::lit(0)).unwrap());

        let cases = vec![PhysicalWhenThen::new(
            PhysicalLiteralExpr::new(false),
            invalid_expr,
        )];

        let expr = PhysicalCaseExpr {
            cases,
            else_expr: Box::new(PhysicalLiteralExpr::new("hello").into()),
            datatype: DataType::Utf8,
        };

        let mut input = generate_batch!([Some(4), Some(5), Some(6), None]);
        let mut state = expr.create_state(4).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 4).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(0, 4), &mut out)
            .unwrap();

        let expected = generate_array!(["hello", "hello", "hello", "hello"]);
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn case_shortcircuit_then_expr_complex() {
        // CASE WHEN a < 3          THEN a / 0
        //      WHEN a > 3 OR a < 7 THEN a
        //      WHEN a IS NULL      THEN 22
        //      WHEN a > 7          THEN 1 / 0
        // END

        let mut table_list = TableList::empty();
        let t0 = table_list
            .push_table(None, [DataType::Int32], ["a"])
            .unwrap();

        // a < 3
        let when_0 = plan_scalar(
            &table_list,
            expr::lt(expr::column((t0, 0), DataType::Int32), expr::lit(3)).unwrap(),
        );
        // a / 0
        let then_0 = plan_scalar(
            &table_list,
            expr::div(expr::column((t0, 0), DataType::Int32), expr::lit(0)).unwrap(),
        );

        // a > 3 OR a < 7
        let when_1 = plan_scalar(
            &table_list,
            expr::or([
                expr::gt(expr::column((t0, 0), DataType::Int32), expr::lit(3))
                    .unwrap()
                    .into(),
                expr::lt(expr::column((t0, 0), DataType::Int32), expr::lit(7))
                    .unwrap()
                    .into(),
            ])
            .unwrap(),
        );
        // a
        let then_1 = plan_scalar(&table_list, expr::column((t0, 0), DataType::Int32));

        // a IS NULL
        let when_2 = plan_scalar(
            &table_list,
            expr::scalar_function(
                &FUNCTION_SET_IS_NULL,
                vec![expr::column((t0, 0), DataType::Int32).into()],
            )
            .unwrap(),
        );
        // 22
        let then_2 = plan_scalar(&table_list, expr::lit(22));

        // a > 7
        let when_3 = plan_scalar(
            &table_list,
            expr::gt(expr::column((t0, 0), DataType::Int32), expr::lit(7)).unwrap(),
        );
        // 1 / 0
        let then_3 = plan_scalar(&table_list, expr::div(expr::lit(1), expr::lit(0)).unwrap());

        let cases = vec![
            PhysicalWhenThen::new(when_0, then_0),
            PhysicalWhenThen::new(when_1, then_1),
            PhysicalWhenThen::new(when_2, then_2),
            PhysicalWhenThen::new(when_3, then_3),
        ];

        let expr = PhysicalCaseExpr {
            cases,
            else_expr: Box::new(typed_null(DataType::Int32)),
            datatype: DataType::Int32,
        };

        let mut input = generate_batch!([Some(4), Some(5), Some(6), None]);
        let mut state = expr.create_state(4).unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 4).unwrap();
        expr.eval(&mut input, &mut state, Selection::linear(0, 4), &mut out)
            .unwrap();

        let expected = generate_array!([4, 5, 6, 22]);
        assert_arrays_eq(&expected, &out);
    }
}
