use std::task::Context;

use rayexec_bullet::batch::BatchOld;
use rayexec_error::Result;

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
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Physical operator for EXPLAIN ANALYZE.
#[derive(Debug)]
pub struct PhysicalAnalyze {}

impl ExecutableOperatorOld for PhysicalAnalyze {
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

impl Explainable for PhysicalAnalyze {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Analyze")
    }
}
