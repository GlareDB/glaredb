
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
        let output = Array::try_new(&NopBufferManager, DataType::Boolean, batch_size)?;
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
        self.output.reset_for_write(&NopBufferManager)?;

        self.evaluator
            .eval_single_expression(input, input.selection(), &mut self.output)?;

        // Provide selection relative to the boolean output array.
        UnaryExecutor::select(&self.output, 0..input.num_rows(), &mut self.selection)?;

        Ok(&self.selection)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;

    #[test]
    fn select_simple() {
        let mut evaluator = SelectionEvaluator::try_new(
            PhysicalScalarExpression::Column(PhysicalColumnExpr {
                datatype: DataType::Boolean,
                idx: 0,
            }),
            1024,
        )
        .unwrap();

        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([true, false, true, true]).unwrap(),
            Array::try_from_iter([8, 9, 7, 6]).unwrap(),
        ])
        .unwrap();

        let selection = evaluator.select(&mut input).unwrap();
        assert_eq!(&[0, 2, 3], selection);

        // Make sure we reset internal state.
        let mut input = Batch::try_from_arrays([
            Array::try_from_iter([true, false, false, false]).unwrap(),
            Array::try_from_iter([2, 2, 2, 2]).unwrap(),
        ])
        .unwrap();

        let selection = evaluator.select(&mut input).unwrap();
        assert_eq!(&[0], selection);
    }
}
