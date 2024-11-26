use std::task::Context;

use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use super::{
    ExecutableOperator,
    ExecutionStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct PhysicalWindow {}

impl ExecutableOperator for PhysicalWindow {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!()
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}

impl Explainable for PhysicalWindow {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Window")
    }
}
