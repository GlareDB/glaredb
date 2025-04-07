use std::fmt::Debug;
use std::marker::PhantomData;
use std::task::{Context, Poll};

use futures::FutureExt;
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
use glaredb_core::logical::statistics::StatisticsValue;
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::runtime::filesystem::{AnyFile, AnyFileSystem, FileSystemFuture, OpenFlags};
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_error::{DbError, Result};

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
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &ParquetMetadataFunction::<FileMetadataTable>::new(),
    )],
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
        MetadataColumn::new("file_name", DataType::Utf8),
        MetadataColumn::new("version", DataType::Int32),
        MetadataColumn::new("num_rows", DataType::Int64),
        MetadataColumn::new("created_by", DataType::Utf8),
        MetadataColumn::new("num_row_groups", DataType::Int64),
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
    fs: AnyFileSystem,
    path: String,
}

pub struct ParquetMetadataOperatorState {
    fs: AnyFileSystem,
    path: String,
    projections: Projections,
}

pub struct ParquetMetadataPartitionState<T: MetadataTable> {
    table_state: T::State,
    scan_state: PartitionScanState,
}

#[derive(Debug)]
pub struct FileWithMetadata {
    #[allow(unused)]
    file: AnyFile,
    metadata: ParquetMetaData,
}

enum PartitionScanState {
    Opening {
        open_fut: FileSystemFuture<'static, Result<FileWithMetadata>>,
    },
    Scanning {
        file: FileWithMetadata,
    },
    Exhausted,
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
        let path = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        // TODO: GLOBBING!
        let fs = scan_context.dispatch.filesystem_for_path(&path)?;
        match fs.call_stat(&path).await? {
            Some(stat) if stat.file_type.is_file() => (), // We have a file.
            Some(_) => return Err(DbError::new("Cannot read parquet from a directory")),
            None => return Err(DbError::new(format!("Missing file for path '{path}'"))),
        }

        Ok(TableFunctionBindState {
            state: ParquetMetadataBindState {
                fs: fs.clone(),
                path,
            },
            input,
            schema: T::column_schema(),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ParquetMetadataOperatorState {
            fs: bind_state.fs.clone(),
            path: bind_state.path.clone(),
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        // One partition scans for now...
        let open_fut = op_state
            .fs
            .call_open_static(OpenFlags::READ, op_state.path.clone());
        let mut states = vec![ParquetMetadataPartitionState {
            table_state: Default::default(),
            scan_state: PartitionScanState::Opening {
                open_fut: Box::pin(async move {
                    let mut file = open_fut.await?;
                    let loader = MetaDataLoader::new();
                    let metadata = loader.load_from_file(&mut file).await?;
                    Ok(FileWithMetadata { file, metadata })
                }),
            },
        }];

        states.resize_with(partitions, || ParquetMetadataPartitionState {
            table_state: Default::default(),
            scan_state: PartitionScanState::Exhausted,
        });

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        loop {
            match &mut state.scan_state {
                PartitionScanState::Opening { open_fut } => {
                    let file = match open_fut.poll_unpin(cx)? {
                        Poll::Ready(file) => file,
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    state.scan_state = PartitionScanState::Scanning { file };
                    continue;
                }
                PartitionScanState::Scanning { file } => {
                    T::scan(&mut state.table_state, &op_state.projections, file, output)?;
                    if output.num_rows() == 0 {
                        // TODO: Move to next file.
                        return Ok(PollPull::Exhausted);
                    }
                    return Ok(PollPull::HasMore);
                }
                PartitionScanState::Exhausted => {
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }
            }
        }
    }
}
