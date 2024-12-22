use std::task::Context;

use rayexec_bullet::batch::BatchOld;
use rayexec_error::Result;

use crate::database::DatabaseContext;
use crate::execution::operators::{
    ExecutableOperatorOld,
    ExecutionStates,
    OperatorStateOld,
    PartitionStateOld,
    PollFinalizeOld,
    PollPullOld,
    PollPushOld,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct TopKPartitionState {}

#[derive(Debug)]
pub struct TopKOperatorState {}

#[derive(Debug)]
pub struct PhysicalTopK {}

impl ExecutableOperatorOld for PhysicalTopK {
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
        _partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
        _batch: BatchOld,
    ) -> Result<PollPushOld> {
        unimplemented!()
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
    ) -> Result<PollFinalizeOld> {
        unimplemented!()
    }

    fn poll_pull_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
    ) -> Result<PollPullOld> {
        unimplemented!()
    }
}

impl Explainable for PhysicalTopK {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TopK")
    }
}
