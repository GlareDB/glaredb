use std::sync::Arc;
use std::task::Context;

use rayexec_bullet::batch::BatchOld;
use rayexec_error::{RayexecError, Result};

use super::{
    ExecutableOperatorOld,
    ExecutionStates,
    OperatorStateOld,
    PartitionStateOld,
    PollFinalizeOld,
    PollPullOld,
    PollPushOld,
};
use crate::database::DatabaseContext;
use crate::execution::operators::InputOutputStates;
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

impl ExecutableOperatorOld for PhysicalEmpty {
    fn create_states_old(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorStateOld::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: (0..partitions[0])
                    .map(|_| PartitionStateOld::Empty(EmptyPartitionState { finished: false }))
                    .collect(),
            },
        })
    }

    fn poll_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
        _batch: BatchOld,
    ) -> Result<PollPushOld> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
    ) -> Result<PollFinalizeOld> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_pull_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
    ) -> Result<PollPullOld> {
        match partition_state {
            PartitionStateOld::Empty(state) => {
                if state.finished {
                    Ok(PollPullOld::Exhausted)
                } else {
                    state.finished = true;
                    Ok(PollPullOld::Computed(
                        BatchOld::empty_with_num_rows(1).into(),
                    ))
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
