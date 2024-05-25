use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use crate::execution::operators::{
    OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush,
};

#[derive(Debug)]
pub struct TopKPartitionState {}

#[derive(Debug)]
pub struct TopKOperatorState {}

#[derive(Debug)]
pub struct PhysicalTopK {}

impl PhysicalTopK {
    pub fn create_states(
        &self,
        input_partitions: usize,
    ) -> (TopKOperatorState, Vec<TopKPartitionState>) {
        unimplemented!()
    }
}

impl PhysicalOperator for PhysicalTopK {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
