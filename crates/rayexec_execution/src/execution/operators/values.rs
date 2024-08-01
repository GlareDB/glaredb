use crate::{
    database::DatabaseContext,
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{sync::Arc, task::Context};

use super::{
    ExecutionStates, InputOutputStates, OperatorState, PartitionState, PhysicalOperator,
    PollFinalize, PollPull, PollPush,
};

#[derive(Debug)]
pub struct ValuesPartitionState {
    batches: Vec<Batch>,
}

#[derive(Debug)]
pub struct PhysicalValues {
    batches: Vec<Batch>,
}

impl PhysicalValues {
    pub fn new(batches: Vec<Batch>) -> Self {
        PhysicalValues { batches }
    }
}

impl PhysicalOperator for PhysicalValues {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let num_partitions = partitions[0];

        let mut states: Vec<_> = (0..num_partitions)
            .map(|_| ValuesPartitionState {
                batches: Vec::new(),
            })
            .collect();

        for (idx, batch) in self.batches.iter().enumerate() {
            states[idx % num_partitions].batches.push(batch.clone());
        }

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states.into_iter().map(PartitionState::Values).collect(),
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
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Values(state) => match state.batches.pop() {
                Some(batch) => Ok(PollPull::Batch(batch)),
                None => Ok(PollPull::Exhausted),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Values")
    }
}
