use crate::{
    database::DatabaseContext,
    execution::operators::InputOutputStates,
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{sync::Arc, task::Context};

use super::{
    ExecutionStates, OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull,
    PollPush,
};

#[derive(Debug, Default)]
pub struct EmptyPartitionState {
    finished: bool,
}

#[derive(Debug)]
pub struct PhysicalEmpty;

impl PhysicalEmpty {
    pub fn create_states(num_partitions: usize) -> Vec<EmptyPartitionState> {
        (0..num_partitions)
            .map(|_| EmptyPartitionState { finished: false })
            .collect()
    }
}

impl PhysicalOperator for PhysicalEmpty {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: (0..partitions[0])
                    .map(|_| PartitionState::Empty(EmptyPartitionState { finished: false }))
                    .collect(),
            },
        })
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical empty"))
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Empty(state) => {
                if state.finished {
                    Ok(PollPull::Exhausted)
                } else {
                    state.finished = true;
                    Ok(PollPull::Batch(Batch::empty_with_num_rows(1)))
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
