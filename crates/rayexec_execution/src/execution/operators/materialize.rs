use std::sync::Arc;
use std::task::Context;

use rayexec_error::Result;

use super::{
    BaseOperator,
    ExecutionProperties,
    PollFinalize,
    PollPull,
    PollPush,
    PullOperator,
    PushOperator,
};
use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ConcurrentColumnCollection,
    ParallelColumnCollectionScanState,
};
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct MaterializeOperatorState {}

#[derive(Debug)]
pub struct MaterializePushPartitionState {}

#[derive(Debug)]
pub struct MaterializePullPartitionState {
    scan_state: ParallelColumnCollectionScanState,
}

#[derive(Debug)]
pub struct PhysicalMaterialize {
    pub(crate) collection: Arc<ConcurrentColumnCollection>,
}

impl BaseOperator for PhysicalMaterialize {
    type OperatorState = MaterializeOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        unimplemented!()
    }

    fn output_types(&self) -> &[DataType] {
        unimplemented!()
    }
}

impl PullOperator for PhysicalMaterialize {
    type PartitionPullState = MaterializePullPartitionState;

    fn create_partition_pull_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        unimplemented!()
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}

impl PushOperator for PhysicalMaterialize {
    type PartitionPushState = MaterializePushPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        unimplemented!()
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalMaterialize {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Materialize")
    }
}
