use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::task::Context;

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct ValuesPartitionState {
    batches: Vec<Batch>,
}

impl ValuesPartitionState {
    pub const fn with_batches(batches: Vec<Batch>) -> Self {
        ValuesPartitionState { batches }
    }

    pub const fn empty() -> Self {
        ValuesPartitionState {
            batches: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct PhysicalValues;

impl PhysicalOperator for PhysicalValues {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
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
