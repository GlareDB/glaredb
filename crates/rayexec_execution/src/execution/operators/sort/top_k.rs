use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch2;
use crate::database::DatabaseContext;
use crate::execution::operators::{
    ExecutableOperator,
    ExecutionStates2,
    OperatorState,
    PartitionState,
    PollFinalize2,
    PollPull2,
    PollPush2,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct TopKPartitionState {}

#[derive(Debug)]
pub struct TopKOperatorState {}

#[derive(Debug)]
pub struct PhysicalTopK {}

impl ExecutableOperator for PhysicalTopK {
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

impl Explainable for PhysicalTopK {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TopK")
    }
}
