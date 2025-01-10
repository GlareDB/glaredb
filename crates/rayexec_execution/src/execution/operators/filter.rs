use std::sync::Arc;
use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub(crate) predicate: PhysicalScalarExpression,
}

#[derive(Debug)]
pub struct FilterPartitionState {
    evaluator: ExpressionEvaluator,
    /// Boolean array for holding the output of the filter expression.
    output: Array,
    /// Selected indices buffer.
    selection: Vec<usize>,
}

impl ExecutableOperator for PhysicalFilter {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let partition_states = (0..partitions)
            .map(|_| {
                Ok(PartitionState::Filter(FilterPartitionState {
                    evaluator: ExpressionEvaluator::try_new(
                        vec![self.predicate.clone()],
                        batch_size,
                    )?,
                    output: Array::try_new(
                        &Arc::new(NopBufferManager),
                        DataType::Boolean,
                        batch_size,
                    )?,
                    selection: Vec::with_capacity(batch_size),
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PartitionAndOperatorStates::Branchless {
            operator_state: OperatorState::None,
            partition_states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Filter(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        let input = inout.input.required("batch input")?;
        let output = inout.output.required("batch output")?;

        state.output.reset_for_write(&Arc::new(NopBufferManager))?;
        state
            .evaluator
            .eval_single_expression(input, input.selection(), &mut state.output)?;

        state.selection.clear();
        UnaryExecutor::select(
            &state.output,
            Selection::linear(input.num_rows()),
            &mut state.selection,
        )?;

        output.try_clone_from(input)?;

        if state.selection.len() != output.num_rows() {
            // Only add selection if we're actually omitting rows.
            output.select(&state.selection)?;
        }

        Ok(PollExecute::Ready)
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", &self.predicate)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::execution::operators::testutil::{test_database_context, OperatorWrapper};
    use crate::expr::physical::column_expr::PhysicalColumnExpr;

    #[test]
    fn filter_simple() {
        let operator = PhysicalFilter {
            predicate: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                datatype: DataType::Boolean,
                idx: 0,
            }),
        };

        let states = operator
            .create_states(&test_database_context(), 4, 1)
            .unwrap();
        let (operator_state, mut partition_states) = states.branchless_into_states().unwrap();
        let wrapper = OperatorWrapper::new(operator);

        let mut out = Batch::try_from_arrays([
            Array::try_new(&Arc::new(NopBufferManager), DataType::Boolean, 4).unwrap(),
            Array::try_new(&Arc::new(NopBufferManager), DataType::Int32, 4).unwrap(),
        ])
        .unwrap();

        let mut in1 = Batch::try_from_arrays([
            Array::try_from_iter([true, false, true, true]).unwrap(),
            Array::try_from_iter([8, 9, 7, 6]).unwrap(),
        ])
        .unwrap();

        wrapper
            .poll_execute(
                &mut partition_states[0],
                &operator_state,
                ExecuteInOutState {
                    input: Some(&mut in1),
                    output: Some(&mut out),
                },
            )
            .unwrap();

        let expected1 = Batch::try_from_arrays([
            Array::try_from_iter([true, true, true]).unwrap(),
            Array::try_from_iter([8, 7, 6]).unwrap(),
        ])
        .unwrap();
        assert_batches_eq(&expected1, &out);

        let mut in2 = Batch::try_from_arrays([
            Array::try_from_iter([true, false, false, false]).unwrap(),
            Array::try_from_iter([4, 3, 2, 1]).unwrap(),
        ])
        .unwrap();

        wrapper
            .poll_execute(
                &mut partition_states[0],
                &operator_state,
                ExecuteInOutState {
                    input: Some(&mut in2),
                    output: Some(&mut out),
                },
            )
            .unwrap();

        let expected2 = Batch::try_from_arrays([
            Array::try_from_iter([true]).unwrap(),
            Array::try_from_iter([4]).unwrap(),
        ])
        .unwrap();
        assert_batches_eq(&expected2, &out);
    }
}
