use std::fmt;

use glaredb_error::Result;

use super::evaluator::ExpressionEvaluator;
use super::{ExpressionState, PhysicalScalarExpression};
use crate::arrays::array::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::functions::cast::PlannedCastFunction;

#[derive(Debug, Clone)]
pub struct PhysicalCastExpr {
    pub to: DataType,
    pub expr: Box<PhysicalScalarExpression>,
    pub cast_function: PlannedCastFunction,
}

impl PhysicalCastExpr {
    pub(crate) fn create_state(&self, batch_size: usize) -> Result<ExpressionState> {
        let inputs = vec![self.expr.create_state(batch_size)?];
        let buffer = Batch::new([self.expr.datatype()], batch_size)?;

        Ok(ExpressionState { buffer, inputs })
    }

    pub fn datatype(&self) -> DataType {
        self.to.clone()
    }

    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        state.reset_for_write()?;

        // Eval child.
        let child_output = &mut state.buffer.arrays_mut()[0];
        ExpressionEvaluator::eval_expression(
            &self.expr,
            input,
            &mut state.inputs[0],
            sel,
            child_output,
        )?;

        // Cast child output.
        //
        // Note we discard the previous selection since the child would have
        // written the rows starting at 0 up to selection len.
        self.cast_function
            .call_cast(child_output, Selection::linear(0, sel.len()), output)?;

        Ok(())
    }
}

impl fmt::Display for PhysicalCastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} TO {})", self.expr, self.to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::expr;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::assert_arrays_eq_sel;
    use crate::testutil::exprs::plan_scalar;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn cast_expr_literal_string_to_i32() {
        let expr = plan_scalar(
            &TableList::empty(),
            expr::cast(expr::lit("35"), DataType::Int32).unwrap(),
        );
        let expr = match expr {
            PhysicalScalarExpression::Cast(cast) => cast,
            other => panic!("unexpected expression: {other:?}"),
        };

        let mut state = expr.create_state(1024).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 1024).unwrap();
        let mut input = Batch::empty_with_num_rows(3);
        let sel = input.selection();

        expr.eval(&mut input, &mut state, sel, &mut out).unwrap();

        let expected = Array::try_from_iter([35, 35, 35]).unwrap();
        assert_arrays_eq_sel(&expected, 0..3, &out, 0..3);
    }
}
