use std::fmt::Debug;
use std::future::Future;
use std::task::Context;

use glaredb_error::Result;

use super::{TableFunctionBindState, TableFunctionInput};
use crate::arrays::batch::Batch;
use crate::catalog::context::DatabaseContext;
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::runtime::filesystem::dispatch::FileSystemDispatch;
use crate::storage::projections::Projections;
use crate::storage::scan_filter::PhysicalScanFilter;

// TODO: `pre_bind` (resolve), consolidate `TableScanFunction` and `TableExecuteFunction`.

/// Context providing dependencies for a scan.
#[derive(Debug, Clone, Copy)]
pub struct ScanContext<'a> {
    pub database_context: &'a DatabaseContext,
    pub dispatch: &'a FileSystemDispatch,
}

/// Scan function that produces batches.
pub trait TableScanFunction: Debug + Copy + Send + Sync + 'static {
    type BindState: Sync + Send;

    type OperatorState: Sync + Send;
    type PartitionState: Sync + Send;

    /// Binds the table function.
    ///
    /// This should determine the output schema of the table.
    fn bind(
        scan_context: ScanContext,
        input: TableFunctionInput,
    ) -> impl Future<Output = Result<TableFunctionBindState<Self::BindState>>> + Send;

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        filters: &[PhysicalScanFilter],
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState>;

    // TODO: Pass bind state here too. This will reduce some extra cloning.
    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>>;

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull>;
}
