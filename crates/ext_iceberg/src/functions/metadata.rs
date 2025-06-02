use std::sync::Arc;
use std::task::Context;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalUtf8,
};
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::{DataType, DataTypeId};
use glaredb_core::arrays::field::{ColumnSchema, Field};
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
use glaredb_core::statistics::value::StatisticsValue;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::Result;

use crate::table::spec;
use crate::table::state::TableState;

pub const FUNCTION_SET_ICEBERG_METADATA: TableFunctionSet = TableFunctionSet {
    name: "iceberg_metadata",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Get the table metadata for an Iceberg table.",
        arguments: &["table_root"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &IcebergMetadata,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct IcebergMetadata;

#[derive(Debug)]
pub struct IcebergMetadataBindState {
    metadata: Arc<spec::Metadata>,
}

#[derive(Debug)]
pub struct IcebergMetadataOperatorState {
    projections: Projections,
    metadata: Arc<spec::Metadata>, // TODO
}

#[derive(Debug)]
pub enum IcebergMetadataPartitionState {
    Scanning,
    Exhausted,
}

impl TableScanFunction for IcebergMetadata {
    type BindState = IcebergMetadataBindState;
    type OperatorState = IcebergMetadataOperatorState;
    type PartitionState = IcebergMetadataPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        // TODO: Remove clone...
        let table = TableState::open_root_with_inputs(scan_context, input.clone()).await?;

        Ok(TableFunctionBindState {
            state: IcebergMetadataBindState {
                metadata: table.metadata.clone(),
            },
            input,
            data_schema: ColumnSchema::new([
                Field::new("format_version", DataType::int32(), false),
                Field::new("table_uuid", DataType::utf8(), false),
                Field::new("location", DataType::utf8(), false),
            ]),
            meta_schema: None,
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(IcebergMetadataOperatorState {
            projections,
            metadata: bind_state.metadata.clone(),
        })
    }

    fn create_pull_partition_states(
        _op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert_ne!(0, partitions);

        let mut states = vec![IcebergMetadataPartitionState::Scanning];
        states.resize_with(partitions, || IcebergMetadataPartitionState::Exhausted);

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        match state {
            IcebergMetadataPartitionState::Scanning => {
                op_state
                    .projections
                    .for_each_column(output, &mut |col, arr| {
                        match col {
                            ProjectedColumn::Data(0) => {
                                // format_version
                                let mut data = PhysicalI32::get_addressable_mut(arr.data_mut())?;
                                data.put(0, &op_state.metadata.format_version);
                                Ok(())
                            }
                            ProjectedColumn::Data(1) => {
                                // table_uuid
                                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                                data.put(0, &op_state.metadata.table_uuid);
                                Ok(())
                            }
                            ProjectedColumn::Data(2) => {
                                // location
                                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                                data.put(0, &op_state.metadata.location);
                                Ok(())
                            }
                            other => panic!("invalid projection: {other:?}"),
                        }
                    })?;

                output.set_num_rows(1)?;
                Ok(PollPull::Exhausted)
            }
            IcebergMetadataPartitionState::Exhausted => {
                output.set_num_rows(0)?;
                Ok(PollPull::Exhausted)
            }
        }
    }
}
