use std::task::Context;

use glaredb_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::execution::operators::{
    BaseOperator,
    ExecuteOperator,
    ExecutionProperties,
    PollExecute,
    PollFinalize,
};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct TopKOperatorState {}

#[derive(Debug)]
pub struct TopKPartitionState {}

#[derive(Debug)]
pub struct PhysicalTopK {}

impl BaseOperator for PhysicalTopK {
    const OPERATOR_NAME: &str = "TopK";

    type OperatorState = TopKOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        unimplemented!()
    }

    fn output_types(&self) -> &[DataType] {
        unimplemented!()
    }
}

impl ExecuteOperator for PhysicalTopK {
    type PartitionExecuteState = TopKPartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalTopK {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf).build()
    }
}
