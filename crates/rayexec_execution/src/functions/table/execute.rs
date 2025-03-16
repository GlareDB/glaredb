use std::fmt::Debug;
use std::task::Context;

use rayexec_error::Result;

use super::{TableFunctionBindState, TableFunctionInput};
use crate::arrays::batch::Batch;
use crate::execution::operators::{ExecutionProperties, PollExecute, PollFinalize};

/// Table function that accepts table inputs and produces table outputs.
pub trait TableExecuteFunction: Debug + Copy + Send + Sync + 'static {
    type BindState: Sync + Send;

    type OperatorState: Sync + Send;
    type PartitionState: Sync + Send;

    fn bind(&self, input: TableFunctionInput) -> Result<TableFunctionBindState<Self::BindState>>;

    fn create_execute_operator_state(
        bind_state: &Self::BindState,
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState>;

    fn create_execute_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>>;

    /// Execute the table function on the input batch, placing results in the
    /// output batch.
    fn poll_execute(
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>;

    fn poll_finalize_execute(
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
    ) -> Result<PollFinalize>;
}
