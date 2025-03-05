use std::fmt::Debug;
use std::future::Future;
use std::task::Context;

use rayexec_error::Result;

use super::{TableFunctionBindState, TableFunctionInput};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::storage::table_storage::Projections;

/// Scan function that produces batches.
pub trait TableScanFunction: Debug + Copy + Send + Sync + 'static {
    type BindState: Sync + Send;

    type OperatorState: Sync + Send;
    type PartitionState: Sync + Send;

    /// Binds the table function.
    ///
    /// This should determine the output schema of the table.
    fn bind(
        &self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> impl Future<Output = Result<TableFunctionBindState<Self::BindState>>> + '_;

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState>;

    fn create_pull_partition_states(
        state: &Self::BindState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>>;

    fn poll_pull(
        &self,
        cx: &mut Context,
        op_state: &Self::OperatorState,
        partition_state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull>;
}
