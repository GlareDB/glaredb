use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::FutureExt;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::DataTypeId;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::buffer::buffer_manager::DefaultBufferManager;
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
use glaredb_core::runtime::filesystem::{FileSystemFuture, FileSystemWithState, OpenFlags};
use glaredb_core::statistics::value::StatisticsValue;
use glaredb_core::storage::projections::Projections;
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{DbError, Result};

use crate::metadata::ParquetMetaData;
use crate::metadata::loader::MetaDataLoader;
use crate::reader::{Reader, ScanRowGroup, ScanUnit};
use crate::schema::convert::ColumnSchemaTypeVisitor;

pub const FUNCTION_SET_READ_PARQUET: TableFunctionSet = TableFunctionSet {
    name: "read",
    aliases: &["scan"],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read a parquet file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ReadParquet,
        ),
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::List], DataTypeId::Table),
            &ReadParquet,
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct ReadParquet;

pub struct ReadParquetBindState {
    fs: FileSystemWithState,
    schema: ColumnSchema,
    first: FirstFile,
    mf_data: MultiFileData,
}

/// Information about the first file we read during bind.
#[derive(Debug, Clone)]
struct FirstFile {
    path: String,
    metadata: Arc<ParquetMetaData>,
}

pub struct ReadParquetOperatorState {
    fs: FileSystemWithState,
    first: FirstFile,
    projections: Projections,
    filters: Vec<PhysicalScanFilter>,
    schema: ColumnSchema,
    mf_data: MultiFileData,
}

pub struct ReadParquetPartitionState {
    /// Current read state.
    state: ReadState,
    /// Reader that's reused across all files.
    reader: Reader,
    /// Queue of file paths this partition is responsible for decoding the
    /// metadata for.
    // TODO: Change this to be a scan queue containing scan units. Parallelism
    // should be based on row groups, not files.
    //
    // The first file is already parallelized with row groups...
    file_queue: VecDeque<String>,
}

enum ReadState {
    /// Init the next file to read.
    Init,
    /// Currently opening a file.
    Opening {
        open_fut: FileSystemFuture<'static, Result<ScanUnit>>,
    },
    /// Currently scanning a file.
    Scanning,
}

impl TableScanFunction for ReadParquet {
    type BindState = ReadParquetBindState;
    type OperatorState = ReadParquetOperatorState;
    type PartitionState = ReadParquetPartitionState;

    async fn bind(
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let (mut provider, fs) =
            MultiFileProvider::try_new_from_inputs(scan_context, &input).await?;

        let mut mf_data = MultiFileData::empty();
        provider.expand_all(&mut mf_data).await?;

        // Use first file to figure out the schema we'll be using.
        let first = mf_data
            .get(0)
            .ok_or_else(|| DbError::new("No files for path, expected at least one file"))?;

        let mut file = fs.open(OpenFlags::READ, first).await?;
        let loader = MetaDataLoader::new();
        let metadata = loader.load_from_file(&mut file).await?;

        let schema =
            ColumnSchemaTypeVisitor.convert_schema(&metadata.file_metadata.schema_descr)?;
        let cardinality = metadata.file_metadata.num_rows as usize;

        Ok(TableFunctionBindState {
            state: ReadParquetBindState {
                fs,
                first: FirstFile {
                    path: first.to_string(),
                    metadata: Arc::new(metadata),
                },
                schema: schema.clone(),
                mf_data,
            },
            input,
            data_schema: schema,
            meta_schema: Some(provider.meta_schema()),
            cardinality: StatisticsValue::Exact(cardinality),
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ReadParquetOperatorState {
            fs: bind_state.fs.clone(),
            first: bind_state.first.clone(),
            projections,
            filters: filters.to_vec(),
            schema: bind_state.schema.clone(),
            mf_data: bind_state.mf_data.clone(), // TODO
        })
    }

    fn create_pull_partition_states(
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut partition_row_groups: Vec<_> = (0..partitions).map(|_| VecDeque::new()).collect();

        for rg in ScanRowGroup::from_metadata(&op_state.first.metadata) {
            let part_idx = rg.idx % partitions;
            partition_row_groups[part_idx].push_back(rg);
        }

        let states = partition_row_groups
            .into_iter()
            .enumerate()
            .map(|(partition_idx, groups)| {
                let projections = op_state.projections.clone();
                let metadata = op_state.first.metadata.clone();
                let schema = op_state.schema.clone();
                let open_fut = op_state
                    .fs
                    .open_static(OpenFlags::READ, op_state.first.path.clone());

                // TODO: How do we want to thread down the manager? Put on
                // props? Request that it goes on op_state?
                let reader = Reader::try_new(
                    &DefaultBufferManager,
                    schema,
                    &metadata.file_metadata.schema_descr,
                    projections,
                    &op_state.filters,
                )?;

                let fut = Box::pin(async move {
                    let file = open_fut.await?;

                    Ok(ScanUnit {
                        metadata,
                        file,
                        row_groups: groups,
                    })
                });

                // Split up files per partition.
                //
                // TODO: See TODO about scan unit queue.

                // Skip first since that's already going onto this partition's
                // state.
                let expanded = &op_state.mf_data.expanded()[1..];
                let file_queue: VecDeque<_> = expanded
                    .iter()
                    .skip(partition_idx)
                    .step_by(partitions)
                    .map(|path| path.to_string())
                    .collect();

                Ok(ReadParquetPartitionState {
                    state: ReadState::Opening { open_fut: fut },
                    reader,
                    file_queue,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        loop {
            match &mut state.state {
                ReadState::Init => {
                    let path = match state.file_queue.pop_front() {
                        Some(path) => path,
                        None => {
                            // No more files, we are done.
                            output.set_num_rows(0)?;
                            return Ok(PollPull::Exhausted);
                        }
                    };

                    // Create scan unit in a future.
                    let open_fut = op_state.fs.open_static(OpenFlags::READ, path);
                    let fut = Box::pin(async move {
                        let mut file = open_fut.await?;

                        let loader = MetaDataLoader::new();
                        let metadata = loader.load_from_file(&mut file).await?;

                        let row_groups = ScanRowGroup::from_metadata(&metadata).collect();

                        Ok(ScanUnit {
                            metadata: Arc::new(metadata),
                            file,
                            row_groups,
                        })
                    });

                    state.state = ReadState::Opening { open_fut: fut };
                    // Continue...
                }
                ReadState::Opening { open_fut } => {
                    let scan_unit = match open_fut.poll_unpin(cx)? {
                        Poll::Ready(reader) => reader,
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    state.reader.prepare(scan_unit)?;
                    state.state = ReadState::Scanning;
                    // Continue...
                }
                ReadState::Scanning => {
                    let poll = state.reader.poll_pull(cx, output)?;
                    if poll == PollPull::Exhausted {
                        // This file (or scan unit) is complete, move to
                        // initializing the next one.
                        state.state = ReadState::Init;
                        continue;
                    }

                    return Ok(poll);
                }
            }
        }
    }
}
