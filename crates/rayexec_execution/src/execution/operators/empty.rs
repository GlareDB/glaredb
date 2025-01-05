use std::sync::Arc;
use std::task::Context;

use rayexec_error::{RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecutionStates2,
    OperatorState,
    PartitionState,
    PollFinalize2,
    PollPull2,
    PollPush2,
};
use crate::arrays::batch::Batch2;
use crate::database::DatabaseContext;
use crate::execution::operators::InputOutputStates2;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Default)]
pub struct EmptyPartitionState {
    finished: bool,
}

/// A dummy operator that produces a single batch containing no columns and a
/// single row for each partition.
#[derive(Debug)]
pub struct PhysicalEmpty;

impl ExecutableOperator for PhysicalEmpty {
    fn create_states2(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates2> {
        Ok(ExecutionStates2 {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates2::OneToOne {
                partition_states: (0..partitions[0])
                    .map(|_| PartitionState::Empty(EmptyPartitionState { finished: false }))
                    .collect(),
            },
        })
    }

    fn poll_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch2,
    ) -> Result<PollPush2> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_finalize_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize2> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_pull2(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull2> {
        match partition_state {
            PartitionState::Empty(state) => {
                if state.finished {
                    Ok(PollPull2::Exhausted)
                } else {
                    state.finished = true;
                    Ok(PollPull2::Computed(Batch2::empty_with_num_rows(1).into()))
                }
            }
            other => panic!("inner join state is not building: {other:?}"),
        }
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
