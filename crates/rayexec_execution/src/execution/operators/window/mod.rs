use std::task::Context;

use rayexec_bullet::batch::BatchOld;
use rayexec_error::Result;

use super::{
    ExecutableOperator,
    ExecutionStates,
    OperatorState,
    PartitionState,
    PollFinalizeOld,
    PollPullOld,
    PollPushOld,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct PhysicalWindow {}

impl ExecutableOperator for PhysicalWindow {
    fn create_states_old(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        unimplemented!()
    }

    fn poll_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: BatchOld,
    ) -> Result<PollPushOld> {
        unimplemented!()
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalizeOld> {
        unimplemented!()
    }

    fn poll_pull_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPullOld> {
        unimplemented!()
    }
}

impl Explainable for PhysicalWindow {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Window")
    }
}
