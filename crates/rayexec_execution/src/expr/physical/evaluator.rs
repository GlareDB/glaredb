use rayexec_error::{RayexecError, Result};

use super::PhysicalScalarExpression;
use crate::arrays::array::exp::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::buffer_manager::NopBufferManager;
use crate::arrays::scalar::OwnedScalarValue;

/// Evaluate expressions on batch inputs.
#[derive(Debug)]
pub struct ExpressionEvaluator {
    expressions: Vec<PhysicalScalarExpression>,
    states: Vec<ExpressionState>,
}

#[derive(Debug)]
pub(crate) struct ExpressionState {
    /// Buffer for writing intermediate results.
    pub(crate) buffer: Batch,
    /// Child states for expressions that contain other input expressions.
    pub(crate) inputs: Vec<ExpressionState>,
}

impl ExpressionState {
    pub(crate) const fn empty() -> Self {
        ExpressionState {
            buffer: Batch::empty(),
            inputs: Vec::new(),
        }
    }
}

impl ExpressionEvaluator {
    pub fn try_new(expressions: Vec<PhysicalScalarExpression>, batch_size: usize) -> Result<Self> {
        let states = expressions
            .iter()
            .map(|expr| expr.create_state(batch_size))
            .collect::<Result<Vec<_>>>()?;

        Ok(ExpressionEvaluator {
            expressions,
            states,
        })
    }

    pub fn num_expressions(&self) -> usize {
        self.expressions.len()
    }

    pub fn try_eval_constant(&mut self) -> Result<OwnedScalarValue> {
        if self.expressions.len() != 1 {
            return Err(RayexecError::new(
                "Single expression for constant eval required",
            ));
        }

        let expr = &self.expressions[0];
        let state = &mut self.states[0];

        let mut input = Batch::empty_with_num_rows(1);
        let mut out = Array::new(&NopBufferManager, expr.datatype(), 1)?;

        Self::eval_expression(expr, &mut input, state, Selection::linear(1), &mut out)?;

        let v = out.get_value(0)?;

        Ok(v.into_owned())
    }

    /// Evaluate the expression on an input batch, writing the results to the
    /// output batch.
    ///
    /// Output batch must contain the same number of arrays as expressions in
    /// this evaluator. Arrays will be written to in the same order as the
    /// expressions.
    ///
    /// `input` is mutable only to allow converting arrays from owned to
    /// managed.
    ///
    /// `output` will have num rows set to the number of logical rows in the
    /// selection.
    pub fn eval_batch(
        &mut self,
        input: &mut Batch,
        sel: Selection,
        output: &mut Batch,
    ) -> Result<()> {
        debug_assert_eq!(self.expressions.len(), output.arrays().len());

        for (idx, expr) in self.expressions.iter().enumerate() {
            let output = &mut output.arrays_mut()[idx];
            let state = &mut self.states[idx];

            Self::eval_expression(expr, input, state, sel, output)?;
        }

        output.set_num_rows(sel.len())?;

        Ok(())
    }

    pub fn eval_single_expression(
        &mut self,
        input: &mut Batch,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        debug_assert_eq!(1, self.expressions.len());
        Self::eval_expression(
            &self.expressions[0],
            input,
            &mut self.states[0],
            sel,
            output,
        )
    }

    pub(crate) fn eval_expression(
        expr: &PhysicalScalarExpression,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        // TODO: Figure out how the manager will be threaded down. Might just
        // keep it on the array/buffer/batch/something else. We might need
        // `Arc<dyn ...>` here, ideally the buffer reuse prevents us from
        // needing to call into it often.
        output.reset_for_write(&NopBufferManager)?;

        match expr {
            PhysicalScalarExpression::Column(expr) => expr.eval(input, state, sel, output),
            PhysicalScalarExpression::Case(expr) => expr.eval(input, state, sel, output),
            PhysicalScalarExpression::Cast(expr) => expr.eval(input, state, sel, output),
            PhysicalScalarExpression::Literal(expr) => expr.eval(input, state, sel, output),
            PhysicalScalarExpression::ScalarFunction(expr) => expr.eval(input, state, sel, output),
        }
    }
}
