use std::task::Context;

use glaredb_core::arrays::batch::Batch;
use glaredb_core::execution::operators::{ExecutionProperties, PollPull};
use glaredb_core::functions::table::scan::{ScanContext, TableScanFunction};
use glaredb_core::functions::table::{TableFunctionBindState, TableFunctionInput};
use glaredb_core::storage::projections::Projections;
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::Result;

#[derive(Debug, Clone, Copy)]
pub struct IcebergMetadata;

#[derive(Debug)]
pub struct IcebergMetadataBindState {}

#[derive(Debug)]
pub struct IcebergMetadataOperatorState {}

#[derive(Debug)]
pub struct IcebergMetadataPartitionState {}

impl TableScanFunction for IcebergMetadata {
    type BindState = IcebergMetadataBindState;
    type OperatorState = IcebergMetadataOperatorState;
    type PartitionState = IcebergMetadataPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        unimplemented!()
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        filters: &[PhysicalScanFilter],
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        unimplemented!()
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        unimplemented!()
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
