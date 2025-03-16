use std::fmt;

use fmtutil::IntoDisplayableSlice;
use rayexec_error::Result;

use super::evaluator::ExpressionEvaluator;
use super::{ExpressionState, PhysicalScalarExpression};
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::functions::scalar::PlannedScalarFunction;

#[derive(Debug, Clone)]
pub struct PhysicalScalarFunctionExpr {
    pub function: PlannedScalarFunction,
    pub inputs: Vec<PhysicalScalarExpression>,
}

impl PhysicalScalarFunctionExpr {
    pub(crate) fn create_state(&self, batch_size: usize) -> Result<ExpressionState> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.create_state(batch_size))
            .collect::<Result<Vec<_>>>()?;

        let buffer = Batch::new(self.inputs.iter().map(|input| input.datatype()), batch_size)?;

        Ok(ExpressionState { buffer, inputs })
    }

    pub fn datatype(&self) -> DataType {
        self.function.state.return_type.clone()
    }

    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        state.reset_for_write()?;

        // Eval children.
        for (child_idx, array) in state.buffer.arrays_mut().iter_mut().enumerate() {
            let expr = &self.inputs[child_idx];
            let child_state = &mut state.inputs[child_idx];
            ExpressionEvaluator::eval_expression(expr, input, child_state, sel, array)?;
        }

        // Eval function with child outputs.
        state.buffer.set_num_rows(sel.len())?;
        self.function.call_execute(&state.buffer, output)?;

        Ok(())
    }
}

impl fmt::Display for PhysicalScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.function.name,
            self.inputs.display_as_list()
        )
    }
}
