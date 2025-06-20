use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Context;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalI64,
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
use glaredb_error::{OptionExt, Result};

use crate::table::spec;
use crate::table::state::TableState;

// TODO: Some of these would benefit from 'snapshot' parameter.

pub const FUNCTION_SET_ICEBERG_METADATA: TableFunctionSet = TableFunctionSet {
    name: "metadata",
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

pub const FUNCTION_SET_ICEBERG_SNAPSHOTS: TableFunctionSet = TableFunctionSet {
    name: "snapshots",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Get the snapshots associated with the table metadata.",
        arguments: &["table_root"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &MetadataFunction::<IcebergSnapshots>::new(),
    )],
};

pub const FUNCTION_SET_ICEBERG_MANIFEST_LIST: TableFunctionSet = TableFunctionSet {
    name: "manifest_list",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "List the manifest files from the manifest list associated with the table metadata.",
        arguments: &["table_root"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &MetadataFunction::<IcebergManifestList>::new(),
    )],
};

pub const FUNCTION_SET_ICEBERG_DATA_FILES: TableFunctionSet = TableFunctionSet {
    name: "data_files",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "List the data files associated with the table metadata.",
        arguments: &["table_root"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &MetadataFunction::<IcebergDataFiles>::new(),
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

/// How much of the "table metadata" do we need to load for a metadata function.
///
/// Some functions are fine with just the metadata json, other need manifests,
/// etc loaded. This enum controls how much we load.
#[derive(Debug, Clone, Copy)]
enum LoadRequirement {
    Metadata,
    ManifestList,
    #[allow(unused)]
    Manifests,
}

trait MetadataScan: Debug + Clone + Copy + Sync + Send + 'static {
    const COLUMNS: &[ScanColumn];
    const LOAD_REQUIREMENT: LoadRequirement;

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
        table_state: &TableState,
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
    const LOAD_REQUIREMENT: LoadRequirement = LoadRequirement::Metadata;

    type State = IcebergMetadataState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        table_state: &TableState,
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
                    data.put(0, &table_state.metadata.format_version);
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    // table_uuid
                    let (data, validity) = arr.data_and_validity_mut();
                    let mut data = PhysicalUtf8::get_addressable_mut(data)?;
                    match table_state.metadata.table_uuid.as_ref() {
                        Some(uuid) => data.put(0, uuid),
                        None => validity.set_invalid(0),
                    }
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    // location
                    let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                    data.put(0, &table_state.metadata.location);
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
pub struct IcebergSnapshots;

#[derive(Debug, Default)]
struct IcebergSnapshotState {
    snapshot_idx: usize,
}

impl MetadataScan for IcebergSnapshots {
    const COLUMNS: &[ScanColumn] = &[
        ScanColumn::new("snapshot_id", DataType::int64(), false),
        ScanColumn::new("sequence_number", DataType::int64(), true), // NULL for v1
        ScanColumn::new("manifest_list", DataType::utf8(), true),    // May be NULL for v1
    ];
    const LOAD_REQUIREMENT: LoadRequirement = LoadRequirement::Metadata;

    type State = IcebergSnapshotState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        table_state: &TableState,
        output: &mut Batch,
    ) -> Result<()> {
        if state.snapshot_idx >= table_state.metadata.snapshots.len() {
            output.set_num_rows(0)?;
            return Ok(());
        }

        let cap = output.write_capacity()?;
        let rem = table_state.metadata.snapshots.len() - state.snapshot_idx;
        let count = usize::min(cap, rem);

        let snapshots_iter = || {
            table_state
                .metadata
                .snapshots
                .iter()
                .skip(state.snapshot_idx)
                .take(count)
        };

        projections.for_each_column(output, &mut |col, arr| match col {
            ProjectedColumn::Data(0) => {
                // snapshot_id
                let mut data = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, snapshot) in snapshots_iter().enumerate() {
                    data.put(idx, &snapshot.snapshot_id);
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                // sequence_number
                let (data, validity) = arr.data_and_validity_mut();
                let mut data = PhysicalI64::get_addressable_mut(data)?;
                for (idx, snapshot) in snapshots_iter().enumerate() {
                    match &snapshot.sequence_number {
                        Some(num) => data.put(idx, num),
                        None => validity.set_invalid(idx),
                    }
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                // manifest_list
                let (data, validity) = arr.data_and_validity_mut();
                let mut data = PhysicalUtf8::get_addressable_mut(data)?;
                for (idx, snapshot) in snapshots_iter().enumerate() {
                    match &snapshot.manifest_list {
                        Some(man) => data.put(idx, man),
                        None => validity.set_invalid(idx),
                    }
                }
                Ok(())
            }
            other => panic!("invalid projection: {other:?}"),
        })?;

        state.snapshot_idx += count;
        output.set_num_rows(count)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IcebergManifestList;

#[derive(Debug, Default)]
struct IcebergManifestListState {
    ent_idx: usize,
}

impl MetadataScan for IcebergManifestList {
    const COLUMNS: &[ScanColumn] = &[
        ScanColumn::new("manifest_path", DataType::utf8(), false),
        ScanColumn::new("manifest_length", DataType::int64(), false),
        ScanColumn::new("content", DataType::utf8(), false),
        ScanColumn::new("sequence_number", DataType::int64(), false),
    ];
    const LOAD_REQUIREMENT: LoadRequirement = LoadRequirement::ManifestList;

    type State = IcebergManifestListState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        table_state: &TableState,
        output: &mut Batch,
    ) -> Result<()> {
        let manifest_list = table_state
            .manifest_list
            .as_ref()
            .required("manifest list")?;
        if state.ent_idx >= manifest_list.entries.len() {
            output.set_num_rows(0)?;
            return Ok(());
        }

        let cap = output.write_capacity()?;
        let rem = manifest_list.entries.len() - state.ent_idx;
        let count = usize::min(cap, rem);

        let ent_iter = || manifest_list.entries.iter().skip(state.ent_idx).take(count);

        projections.for_each_column(output, &mut |col, arr| match col {
            ProjectedColumn::Data(0) => {
                // manifest_path
                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in ent_iter().enumerate() {
                    data.put(idx, &ent.manifest_path);
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                // manifest_length
                let mut data = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in ent_iter().enumerate() {
                    data.put(idx, &ent.manifest_length);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                // content
                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in ent_iter().enumerate() {
                    data.put(idx, ent.content.as_str());
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                // sequence_number
                let mut data = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in ent_iter().enumerate() {
                    data.put(idx, &ent.sequence_number);
                }
                Ok(())
            }
            other => panic!("invalid projection: {other:?}"),
        })?;

        state.ent_idx += count;
        output.set_num_rows(count)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IcebergDataFiles;

#[derive(Debug, Default)]
pub struct IcebergDataFilesState {
    flattened_idx: usize,
}

impl MetadataScan for IcebergDataFiles {
    const COLUMNS: &[ScanColumn] = &[
        ScanColumn::new("status", DataType::utf8(), false),
        ScanColumn::new("content", DataType::utf8(), false),
        ScanColumn::new("file_path", DataType::utf8(), false),
        ScanColumn::new("file_format", DataType::utf8(), false),
        ScanColumn::new("record_count", DataType::int64(), false),
    ];
    const LOAD_REQUIREMENT: LoadRequirement = LoadRequirement::Manifests;

    type State = IcebergDataFilesState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        table_state: &TableState,
        output: &mut Batch,
    ) -> Result<()> {
        let manifests = table_state.manifests.as_ref().required("manifests")?;
        let total_count: usize = manifests.iter().map(|man| man.entries.len()).sum();
        if state.flattened_idx >= total_count {
            output.set_num_rows(0)?;
            return Ok(());
        }

        let cap = output.write_capacity()?;
        let rem = total_count - state.flattened_idx;
        let count = usize::min(cap, rem);

        struct FlattenedManifest<'a> {
            #[allow(unused)]
            metadata: &'a spec::ManifestMetadata,
            entry: &'a spec::ManifestEntry,
        }

        let flattened_iter = || {
            // Associate the manifest metadata with every entry.
            manifests
                .iter()
                .flat_map(|man| {
                    man.entries.iter().map(|entry| FlattenedManifest {
                        metadata: &man.metadata,
                        entry,
                    })
                })
                .skip(state.flattened_idx)
                .take(count)
        };

        projections.for_each_column(output, &mut |col, arr| match col {
            ProjectedColumn::Data(0) => {
                // status
                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in flattened_iter().enumerate() {
                    data.put(idx, ent.entry.status.as_str());
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                // content
                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in flattened_iter().enumerate() {
                    data.put(idx, ent.entry.data_file.content.as_str());
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                // file_path
                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in flattened_iter().enumerate() {
                    data.put(idx, &ent.entry.data_file.file_path);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                // file_format
                let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in flattened_iter().enumerate() {
                    data.put(idx, &ent.entry.data_file.file_format);
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                // record_count
                let mut data = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, ent) in flattened_iter().enumerate() {
                    data.put(idx, &ent.entry.data_file.record_count);
                }
                Ok(())
            }
            other => panic!("invalid projection: {other:?}"),
        })?;

        state.flattened_idx += count;
        output.set_num_rows(count)?;

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
    table_state: Arc<TableState>,
}

#[derive(Debug)]
struct MetadataFunctionOperatorState {
    projections: Projections,
    table_state: Arc<TableState>, // TODO
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
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        // TODO: Remove clone...
        let mut table = TableState::open_root_with_inputs(scan_context, input.clone()).await?;

        match F::LOAD_REQUIREMENT {
            LoadRequirement::Metadata => {
                // We already have it.
            }
            LoadRequirement::ManifestList => {
                table.load_manifest_list().await?;
            }
            LoadRequirement::Manifests => {
                table.load_manifest_list().await?;
                table.load_manifests().await?;
            }
        }

        Ok(TableFunctionBindState {
            state: MetadataFunctionBindState {
                table_state: Arc::new(table),
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
            table_state: bind_state.table_state.clone(),
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
                    &op_state.table_state,
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
