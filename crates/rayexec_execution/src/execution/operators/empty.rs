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
    UnaryInputStates,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

/// A dummy operator that produces a single batch containing no columns and a
/// single row for each partition.
#[derive(Debug)]
pub struct PhysicalEmpty;

impl ExecutableOperator for PhysicalEmpty {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let states = (0..partitions).map(|_| PartitionState::None).collect();

        Ok(UnaryInputStates {
            operator_state: OperatorState::None,
            partition_states: states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let output = inout.output.required("output batch required")?;
        output.set_num_rows(1)?;
        Ok(PollExecute::Exhausted)
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

impl Explainable for PhysicalEmpty {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Empty")
    }
}

impl DatabaseProtoConv for PhysicalEmpty {
    type ProtoType = rayexec_proto::generated::execution::PhysicalEmpty;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::execution::operators::testutil::{test_database_context, OperatorWrapper};

    #[test]
    fn empty_simple() {
        let mut wrapper = OperatorWrapper::new(PhysicalEmpty);
        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();

        let mut output = Batch::try_from_arrays([
            Array::try_new(&NopBufferManager, DataType::Utf8, 1024).unwrap(),
            Array::try_new(&NopBufferManager, DataType::Int32, 1024).unwrap(),
        ])
        .unwrap();

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
        assert_eq!(1, output.num_rows());
    }
}
