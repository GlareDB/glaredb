use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::Context;

use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct TopKPartitionState {}

#[derive(Debug)]
pub struct TopKOperatorState {}

#[derive(Debug)]
pub struct PhysicalTopK {}

impl PhysicalTopK {
    pub fn create_states(
        &self,
        _input_partitions: usize,
    ) -> (TopKOperatorState, Vec<TopKPartitionState>) {
        unimplemented!()
    }
}

impl PhysicalOperator for PhysicalTopK {
    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn finalize_push(
        &self,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
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

impl Explainable for PhysicalTopK {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("TopK")
    }
}
