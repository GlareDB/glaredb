use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::task::{Context, Poll};

use futures::FutureExt;
use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI16,
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
use glaredb_core::runtime::filesystem::file_provider::{MultiFileData, MultiFileProvider};
use glaredb_core::runtime::filesystem::{
    AnyFile,
    FileSystemFuture,
    FileSystemWithState,
    OpenFlags,
};
use glaredb_core::statistics::value::StatisticsValue;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::Result;

use crate::metadata::ParquetMetaData;
use crate::metadata::loader::MetaDataLoader;

pub const FUNCTION_SET_PARQUET_FILE_METADATA: TableFunctionSet = TableFunctionSet {
    name: "parquet_file_metadata",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Get the file-level metadata for a parquet file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ParquetMetadataFunction::<FileMetadataTable>::new(),
        ),
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::List], DataTypeId::Table),
            &ParquetMetadataFunction::<FileMetadataTable>::new(),
        ),
    ],
};

pub const FUNCTION_SET_PARQUET_ROWGROUP_METADATA: TableFunctionSet = TableFunctionSet {
    name: "parquet_rowgroup_metadata",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Get the metadata for all row groups in a file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ParquetMetadataFunction::<RowGroupMetadataTable>::new(),
        ),
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::List], DataTypeId::Table),
            &ParquetMetadataFunction::<RowGroupMetadataTable>::new(),
        ),
    ],
};

#[derive(Debug)]
pub struct MetadataColumn {
    pub name: &'static str,
    pub datatype: DataType,
}

impl MetadataColumn {
    pub const fn new(name: &'static str, datatype: DataType) -> Self {
        MetadataColumn { name, datatype }
    }
}

pub trait MetadataTable: Debug + Clone + Copy + Sync + Send + 'static {
    /// Columns in the table.
    const COLUMNS: &[MetadataColumn];

    /// Mutable state passed to `scan`.
    ///
    /// A new state is created for every file.
    type State: Default + Sync + Send;

    /// Output schema of the table.
    fn column_schema() -> ColumnSchema {
        ColumnSchema::new(
            Self::COLUMNS
                .iter()
                .map(|c| Field::new(c.name.to_string(), c.datatype.clone(), true)),
        )
    }

    /// Scan the table, updating state as needed.
    ///
    /// If the output batch has zero rows, the file is considered exhausted and
    /// and the next file will be loaded (and new state created).
    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        file: &FileWithMetadata,
        output: &mut Batch,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct FileMetadataTable;

#[derive(Debug, Default)]
pub struct FileMetadataTableState {
    finished: bool,
}

impl MetadataTable for FileMetadataTable {
    const COLUMNS: &[MetadataColumn] = &[
        MetadataColumn::new("filename", DataType::utf8()),
        MetadataColumn::new("version", DataType::int32()),
        MetadataColumn::new("num_rows", DataType::int64()),
        MetadataColumn::new("created_by", DataType::utf8()),
        MetadataColumn::new("num_row_groups", DataType::int64()),
    ];

    type State = FileMetadataTableState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        file: &FileWithMetadata,
        output: &mut Batch,
    ) -> Result<()> {
        if state.finished {
            output.set_num_rows(0)?;
            return Ok(());
        }

        projections.for_each_column(output, &mut |col, arr| match col {
            ProjectedColumn::Data(0) => {
                let mut names = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                names.put(0, file.file.call_path());
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut versions = PhysicalI32::get_addressable_mut(arr.data_mut())?;
                versions.put(0, &file.metadata.file_metadata.version);
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut num_rows = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                num_rows.put(0, &file.metadata.file_metadata.num_rows);
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let (data, validity) = arr.data_and_validity_mut();
                let mut created_by = PhysicalUtf8::get_addressable_mut(data)?;
                match &file.metadata.file_metadata.created_by {
                    Some(s) => created_by.put(0, s),
                    None => validity.set_invalid(0),
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut num_row_groups = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                num_row_groups.put(0, &(file.metadata.row_groups.len() as i64));
                Ok(())
            }
            other => panic!("invalid projection: {other:?}"),
        })?;

        output.set_num_rows(1)?;
        state.finished = true;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RowGroupMetadataTable;

#[derive(Debug, Default)]
pub struct RowGroupMetadataTableState {
    row_group_offset: usize,
}

impl MetadataTable for RowGroupMetadataTable {
    const COLUMNS: &[MetadataColumn] = &[
        MetadataColumn::new("filename", DataType::utf8()),
        MetadataColumn::new("num_rows", DataType::int64()),
        MetadataColumn::new("num_columns", DataType::int64()),
        MetadataColumn::new("uncompressed_size", DataType::int64()),
        MetadataColumn::new("ordinal", DataType::int16()),
    ];

    type State = RowGroupMetadataTableState;

    fn scan(
        state: &mut Self::State,
        projections: &Projections,
        file: &FileWithMetadata,
        output: &mut Batch,
    ) -> Result<()> {
        let cap = output.write_capacity()?;
        let rem = file.metadata.row_groups.len() - state.row_group_offset;
        if rem == 0 {
            output.set_num_rows(0)?;
            return Ok(());
        }

        let count = usize::min(cap, rem);
        let row_groups =
            &file.metadata.row_groups[state.row_group_offset..(state.row_group_offset + count)];

        projections.for_each_column(output, &mut |col, arr| match col {
            ProjectedColumn::Data(0) => {
                let names = PhysicalUtf8::buffer_downcast_mut(arr.data_mut())?;
                names.put_duplicated(file.file.call_path().as_bytes(), 0..count)?;
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut num_rows = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, row_group) in row_groups.iter().enumerate() {
                    num_rows.put(idx, &row_group.num_rows);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut num_cols = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, row_group) in row_groups.iter().enumerate() {
                    num_cols.put(idx, &(row_group.num_columns() as i64));
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut uncompressed_sizes = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                for (idx, row_group) in row_groups.iter().enumerate() {
                    uncompressed_sizes.put(idx, &row_group.total_byte_size);
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut ordinals = PhysicalI16::get_addressable_mut(arr.data_mut())?;
                for (idx, row_group) in row_groups.iter().enumerate() {
                    ordinals.put(idx, &row_group.ordinal.unwrap_or(0));
                }
                Ok(())
            }

            other => panic!("invalid projection: {other:?}"),
        })?;

        output.set_num_rows(count)?;
        state.row_group_offset += count;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParquetMetadataFunction<T> {
    _t: PhantomData<T>,
}

impl<T> ParquetMetadataFunction<T>
where
    T: MetadataTable,
{
    pub const fn new() -> Self {
        ParquetMetadataFunction { _t: PhantomData }
    }
}

pub struct ParquetMetadataBindState {
    fs: FileSystemWithState,
    mf_data: MultiFileData,
}

pub struct ParquetMetadataOperatorState {
    fs: FileSystemWithState,
    mf_data: MultiFileData,
    projections: Projections,
}

pub struct ParquetMetadataPartitionState<T: MetadataTable> {
    read_state: ReadState<T>,
    /// Files this partition will handle.
    file_queue: VecDeque<String>,
}

#[derive(Debug)]
pub struct FileWithMetadata {
    file: AnyFile,
    metadata: ParquetMetaData,
}

enum ReadState<T: MetadataTable> {
    Init,
    Opening {
        open_fut: FileSystemFuture<'static, Result<FileWithMetadata>>,
    },
    Scanning {
        file: FileWithMetadata,
        file_state: T::State,
    },
}

impl<T> TableScanFunction for ParquetMetadataFunction<T>
where
    T: MetadataTable,
{
    type BindState = ParquetMetadataBindState;
    type OperatorState = ParquetMetadataOperatorState;
    type PartitionState = ParquetMetadataPartitionState<T>;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let (mut provider, fs) =
            MultiFileProvider::try_new_from_inputs(scan_context, &input).await?;

        let mut mf_data = MultiFileData::empty();
        provider.expand_all(&mut mf_data).await?;

        Ok(TableFunctionBindState {
            state: ParquetMetadataBindState {
                fs: fs.clone(),
                mf_data,
            },
            input,
            data_schema: T::column_schema(),
            meta_schema: None, // TODO: I think None is fine, but may be inconsistent with some other function.
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ParquetMetadataOperatorState {
            fs: bind_state.fs.clone(),
            mf_data: bind_state.mf_data.clone(),
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut partition_files: Vec<_> = (0..partitions).map(|_| VecDeque::new()).collect();

        for (idx, file) in op_state.mf_data.expanded().iter().enumerate() {
            let part_idx = idx % partitions;
            partition_files[part_idx].push_back(file.clone());
        }

        let states = partition_files
            .into_iter()
            .map(|file_queue| ParquetMetadataPartitionState {
                read_state: ReadState::Init,
                file_queue,
            })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        loop {
            match &mut state.read_state {
                ReadState::Init => {
                    let file = match state.file_queue.pop_front() {
                        Some(file) => file,
                        None => {
                            // We're done.
                            output.set_num_rows(0)?;
                            return Ok(PollPull::Exhausted);
                        }
                    };

                    let open_fut = op_state.fs.open_static(OpenFlags::READ, file);
                    let fut = Box::pin(async move {
                        let mut file = open_fut.await?;

                        let loader = MetaDataLoader::new();
                        let metadata = loader.load_from_file(&mut file).await?;

                        Ok(FileWithMetadata { file, metadata })
                    });

                    state.read_state = ReadState::Opening { open_fut: fut };
                    // Continue...
                }
                ReadState::Opening { open_fut } => {
                    let file = match open_fut.poll_unpin(cx)? {
                        Poll::Ready(file) => file,
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    state.read_state = ReadState::Scanning {
                        file,
                        file_state: Default::default(),
                    };

                    // Continue...
                }
                ReadState::Scanning { file, file_state } => {
                    T::scan(file_state, &op_state.projections, file, output)?;
                    if output.num_rows() == 0 {
                        // Try to get the next file to read.
                        state.read_state = ReadState::Init;
                        continue;
                    }
                    return Ok(PollPull::HasMore);
                }
            }
        }
    }
}
