pub mod operation;

use std::fmt::Debug;
use std::task::Context;

use operation::{PartitionSource, SourceOperation};
use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct SourcePartitionState {
    source: Box<dyn PartitionSource>,
}

/// Operater for reading batches from a source.
// TODO: Deduplicate with table scan and table function scan.
#[derive(Debug)]
pub struct PhysicalSource<S: SourceOperation> {
    pub(crate) source: S,
}

impl<S: SourceOperation> PhysicalSource<S> {
    pub fn new(source: S) -> Self {
        PhysicalSource { source }
    }
}

impl<S: SourceOperation> ExecutableOperator for PhysicalSource<S> {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let sources = self
            .source
            .create_partition_sources(context, batch_size, partitions)?;
        let part_states = sources
            .into_iter()
            .map(|source| PartitionState::Source(SourcePartitionState { source }))
            .collect();

        Ok(UnaryInputStates {
            operator_state: OperatorState::None,
            partition_states: part_states,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Source(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let output = inout.output.required("output batch required")?;
        output.reset_for_write()?;

        match state.source.poll_pull(cx, output)? {
            operation::PollPull::HasMore => Ok(PollExecute::HasMore),
            operation::PollPull::Pending => Ok(PollExecute::Pending),
            operation::PollPull::Exhausted => Ok(PollExecute::Exhausted),
        }
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

impl<S: SourceOperation> Explainable for PhysicalSource<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.source.explain_entry(conf)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::execution::operators::testutil::{
        test_database_context,
        BatchesSource,
        OperatorWrapper,
    };

    #[test]
    fn source_single_partition() {
        let batches = vec![
            Batch::try_from_arrays([Array::try_from_iter([1]).unwrap()]).unwrap(),
            Batch::try_from_arrays([Array::try_from_iter([2]).unwrap()]).unwrap(),
        ];

        let mut wrapper = OperatorWrapper::new(PhysicalSource {
            source: BatchesSource { batches },
        });
        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();

        let mut output = Batch::try_new([DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([1]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([2]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn source_multiple_partitions() {
        let batches = vec![
            Batch::try_from_arrays([Array::try_from_iter([1]).unwrap()]).unwrap(),
            Batch::try_from_arrays([Array::try_from_iter([2]).unwrap()]).unwrap(),
        ];

        let mut wrapper = OperatorWrapper::new(PhysicalSource {
            source: BatchesSource { batches },
        });
        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 2)
            .unwrap();

        let mut output = Batch::try_new([DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([1]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[1],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::try_from_arrays([Array::try_from_iter([2]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &output);
    }
}
