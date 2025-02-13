use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::array::selection::Selection;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::selection_evaluator::SelectionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub(crate) predicate: PhysicalScalarExpression,
}

#[derive(Debug)]
pub struct FilterPartitionState {
    evaluator: SelectionEvaluator,
}

impl ExecutableOperator for PhysicalFilter {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let partition_states = (0..partitions)
            .map(|_| {
                Ok(PartitionState::Filter(FilterPartitionState {
                    evaluator: SelectionEvaluator::try_new(self.predicate.clone(), batch_size)?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(UnaryInputStates {
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

        // TODO: "select_from"
        let selection = state.evaluator.select(input)?;
        output.clone_from_other(input)?;

        if selection.len() != output.num_rows() {
            // Only add selection if we're actually omitting rows.
            output.select(Selection::slice(selection))?;
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
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::database_context::test_database_context;
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn filter_simple() {
        let mut operator = PhysicalFilter {
            predicate: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                datatype: DataType::Boolean,
                idx: 0,
            }),
        };

        let mut states = operator
            .create_states(&test_database_context(), 4, 1)
            .unwrap();
        let wrapper = OperatorWrapper::new(operator);

        let mut out = Batch::from_arrays([
            Array::new(&NopBufferManager, DataType::Boolean, 4).unwrap(),
            Array::new(&NopBufferManager, DataType::Int32, 4).unwrap(),
        ])
        .unwrap();

        let mut in1 = Batch::from_arrays([
            Array::try_from_iter([true, false, true, true]).unwrap(),
            Array::try_from_iter([8, 9, 7, 6]).unwrap(),
        ])
        .unwrap();

        wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut in1),
                    output: Some(&mut out),
                },
            )
            .unwrap();

        let expected1 = Batch::from_arrays([
            Array::try_from_iter([true, true, true]).unwrap(),
            Array::try_from_iter([8, 7, 6]).unwrap(),
        ])
        .unwrap();
        assert_batches_eq(&expected1, &out);

        let mut in2 = Batch::from_arrays([
            Array::try_from_iter([true, false, false, false]).unwrap(),
            Array::try_from_iter([4, 3, 2, 1]).unwrap(),
        ])
        .unwrap();

        wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut in2),
                    output: Some(&mut out),
                },
            )
            .unwrap();

        let expected2 = Batch::from_arrays([
            Array::try_from_iter([true]).unwrap(),
            Array::try_from_iter([4]).unwrap(),
        ])
        .unwrap();
        assert_batches_eq(&expected2, &out);
    }
}
