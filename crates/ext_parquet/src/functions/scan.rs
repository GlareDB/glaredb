use std::fmt::Debug;
use std::task::Context;

use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::DataTypeId;
use glaredb_core::execution::operators::{ExecutionProperties, PollPull};
use glaredb_core::functions::Signature;
use glaredb_core::functions::documentation::{Category, Documentation};
use glaredb_core::functions::function_set::TableFunctionSet;
use glaredb_core::functions::table::scan::{ScanContext, TableScanFunction};
use glaredb_core::functions::table::{
    RawTableFunction,
    TableFunctionBindState,
    TableFunctionInput,
};
use glaredb_core::storage::projections::Projections;
use glaredb_error::{Result, not_implemented};

pub const FUNCTION_SET_READ_PARQUET: TableFunctionSet = TableFunctionSet {
    name: "read_parquet",
    aliases: &["parquet_scan"],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read a parquet file",
        arguments: &["path"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &ReadParquet,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct ReadParquet;

pub struct ReadParquetBindState {}

pub struct ReadParquetOperatorState {}

pub struct ReadParquetPartitionState {}

impl TableScanFunction for ReadParquet {
    type BindState = ReadParquetBindState;
    type OperatorState = ReadParquetOperatorState;
    type PartitionState = ReadParquetPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        not_implemented!("bind")
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        not_implemented!("op state")
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        not_implemented!("partition state")
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        not_implemented!("poll pull")
    }
}
