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
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

/// Unions two input operations.
///
/// This operator does nothing special other than producing states that
/// influence pipeline execution. Batches are passed through as-is.
#[derive(Debug)]
pub struct PhysicalUnion;

impl ExecutableOperator for PhysicalUnion {
    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        // 2 child inputs, each with n partitions.
        let states = (0..2)
            .map(|_| {
                (0..partitions)
                    .map(|_| PartitionState::None)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Ok(PartitionAndOperatorStates::NaryInput {
            operator_state: OperatorState::None,
            input_partition_states: states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let input = inout.input.required("input batch required")?;
        let output = inout.output.required("output batch required")?;

        output.try_clone_from(input)?;

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

impl Explainable for PhysicalUnion {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Union")
    }
}

impl DatabaseProtoConv for PhysicalUnion {
    type ProtoType = rayexec_proto::generated::execution::PhysicalUnion;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self)
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
    use crate::execution::operators::testutil::{test_database_context, OperatorWrapper};

    #[test]
    fn union_simple() {
        let mut operator = OperatorWrapper::new(PhysicalUnion);
        let (op_state, mut input_states) = operator
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap()
            .nary_into_states()
            .unwrap();

        let mut top_input =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();

        let mut out = Batch::try_new([DataType::Int32], 1024).unwrap();

        let poll = operator
            .poll_execute(
                &mut input_states[0][0],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut top_input),
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &out);

        let mut bottom_input =
            Batch::try_from_arrays([Array::try_from_iter([5, 6, 7, 8]).unwrap()]).unwrap();

        let poll = operator
            .poll_execute(
                &mut input_states[0][0],
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut bottom_input),
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([5, 6, 7, 8]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &out);
    }
}
