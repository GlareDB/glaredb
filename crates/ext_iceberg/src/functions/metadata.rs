use std::fmt::Debug;
use std::marker::PhantomData;
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
        &MetadataFunction::<IcebergMetadata>::new(),
    )],
};

#[derive(Debug)]
struct ScanColumn {
    name: &'static str,
    datatype: DataType,
    nullable: bool,
}

impl ScanColumn {
    pub const fn new(name: &'static str, datatype: DataType, nullable: bool) -> Self {
        ScanColumn {
            name,
            datatype,
            nullable,
        }
    }
}

trait MetadataScan: Debug + Clone + Copy + Sync + Send + 'static {
    const COLUMNS: &[ScanColumn];
    type State: Default + Sync + Send;

    fn column_schema() -> ColumnSchema {
        ColumnSchema::new(
            Self::COLUMNS
                .iter()
                .map(|c| Field::new(c.name.to_string(), c.datatype.clone(), c.nullable)),
        )
    }

    /// Scan the metadata, updating state as needed.
    ///
    /// An output batch of zero rows indicates exhaustion.
    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        metadata: &spec::Metadata,
        output: &mut Batch,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct IcebergMetadata;

#[derive(Debug, Default)]
struct IcebergMetadataState {
    finished: bool,
}

impl MetadataScan for IcebergMetadata {
    const COLUMNS: &[ScanColumn] = &[
        ScanColumn::new("format_version", DataType::int32(), false),
        ScanColumn::new("table_uuid", DataType::utf8(), true), // May be NULL for v1 tables.
        ScanColumn::new("location", DataType::utf8(), false),
    ];

    type State = IcebergMetadataState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        metadata: &spec::Metadata,
        output: &mut Batch,
    ) -> Result<()> {
        if state.finished {
            output.set_num_rows(0)?;
            return Ok(());
        }

        projections.for_each_column(output, &mut |col, arr| {
            match col {
                ProjectedColumn::Data(0) => {
                    // format_version
                    let mut data = PhysicalI32::get_addressable_mut(arr.data_mut())?;
                    data.put(0, &metadata.format_version);
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    // table_uuid
                    let (data, validity) = arr.data_and_validity_mut();
                    let mut data = PhysicalUtf8::get_addressable_mut(data)?;
                    match metadata.table_uuid.as_ref() {
                        Some(uuid) => data.put(0, uuid),
                        None => validity.set_invalid(0),
                    }
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    // location
                    let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                    data.put(0, &metadata.location);
                    Ok(())
                }
                other => panic!("invalid projection: {other:?}"),
            }
        })?;

        state.finished = true;
        output.set_num_rows(1)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct MetadataFunction<F: MetadataScan> {
    _f: PhantomData<F>,
}

impl<F> MetadataFunction<F>
where
    F: MetadataScan,
{
    pub const fn new() -> Self {
        MetadataFunction { _f: PhantomData }
    }
}

#[derive(Debug)]
struct MetadataFunctionBindState {
    metadata: Arc<spec::Metadata>,
}

#[derive(Debug)]
struct MetadataFunctionOperatorState {
    projections: Projections,
    metadata: Arc<spec::Metadata>, // TODO
}

#[derive(Debug)]
enum MetadataFunctionPartitionState<F: MetadataScan> {
    Scanning { scan_state: F::State },
    Exhausted,
}

impl<F> TableScanFunction for MetadataFunction<F>
where
    F: MetadataScan,
{
    type BindState = MetadataFunctionBindState;
    type OperatorState = MetadataFunctionOperatorState;
    type PartitionState = MetadataFunctionPartitionState<F>;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        // TODO: Remove clone...
        let table = TableState::open_root_with_inputs(scan_context, input.clone()).await?;

        Ok(TableFunctionBindState {
            state: MetadataFunctionBindState {
                metadata: table.metadata.clone(),
            },
            input,
            data_schema: F::column_schema(),
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
        Ok(MetadataFunctionOperatorState {
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

        let mut states = vec![MetadataFunctionPartitionState::Scanning {
            scan_state: Default::default(),
        }];
        states.resize_with(partitions, || MetadataFunctionPartitionState::Exhausted);

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        match state {
            MetadataFunctionPartitionState::Scanning { scan_state } => {
                F::scan(
                    scan_state,
                    &op_state.projections,
                    &op_state.metadata,
                    output,
                )?;
                if output.num_rows() == 0 {
                    *state = MetadataFunctionPartitionState::Exhausted;
                    return Ok(PollPull::Exhausted);
                }
                Ok(PollPull::HasMore)
            }
            MetadataFunctionPartitionState::Exhausted => {
                output.set_num_rows(0)?;
                Ok(PollPull::Exhausted)
            }
        }
    }
}
