use std::sync::Arc;

use rayexec_error::Result;

use super::evaluator::ExpressionEvaluator;
use super::PhysicalScalarExpression;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::scalar::UnaryExecutor;

/// Wrapper around an expression evaluator for computing selections on batches.
#[derive(Debug)]
pub struct SelectionEvaluator {
    pub(crate) selection: Vec<usize>,
    pub(crate) output: Array,
    pub(crate) evaluator: ExpressionEvaluator,
}

impl SelectionEvaluator {
    pub fn try_new(expression: PhysicalScalarExpression, batch_size: usize) -> Result<Self> {
        let evaluator = ExpressionEvaluator::try_new([expression], batch_size)?;
        let output = Array::try_new(&Arc::new(NopBufferManager), DataType::Boolean, batch_size)?;
        let selection = Vec::with_capacity(batch_size);

        Ok(SelectionEvaluator {
            selection,
            output,
            evaluator,
        })
    }

    pub fn selection(&self) -> &[usize] {
        &self.selection
    }

    /// Select rows from the input based on the expression evaluating to 'true'
    ///
    /// The internal state is cleared across calls to this method.
    pub fn select(&mut self, input: &mut Batch) -> Result<&[usize]> {
        self.selection.clear();
        self.output.reset_for_write(&Arc::new(NopBufferManager))?;

        self.evaluator
            .eval_single_expression(input, input.selection(), &mut self.output)?;

        // Provide selection relative to the boolean output array.
        UnaryExecutor::select(&self.output, 0..input.num_rows(), &mut self.selection)?;

        Ok(&self.selection)
    }
}
