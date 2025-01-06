use std::task::Context;

use rayexec_error::Result;

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
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Physical operator for EXPLAIN ANALYZE.
#[derive(Debug)]
pub struct PhysicalAnalyze {}

impl ExecutableOperator for PhysicalAnalyze {
    fn create_states2(
        &self,
        _context: &DatabaseContext,
        _partitions: Vec<usize>,
    ) -> Result<ExecutionStates2> {
        unimplemented!()
    }

    fn poll_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch2,
    ) -> Result<PollPush2> {
        unimplemented!()
    }

    fn poll_finalize_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize2> {
        unimplemented!()
    }

    fn poll_pull2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull2> {
        unimplemented!()
    }
}

impl Explainable for PhysicalAnalyze {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Analyze")
    }
}
