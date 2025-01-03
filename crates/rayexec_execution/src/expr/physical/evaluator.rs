use rayexec_error::{RayexecError, Result};

use super::PhysicalScalarExpression;
use crate::arrays::array::exp::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::buffer_manager::NopBufferManager;

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
    pub fn new(expressions: Vec<PhysicalScalarExpression>, batch_size: usize) -> Self {
        unimplemented!()
    }

    pub fn num_expressions(&self) -> usize {
        self.expressions.len()
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

        Ok(())
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
