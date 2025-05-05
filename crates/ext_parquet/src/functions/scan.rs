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
use glaredb_core::logical::statistics::StatisticsValue;
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::runtime::filesystem::{
    FileOpenContext,
    FileSystemFuture,
    FileSystemWithState,
    OpenFlags,
};
use glaredb_core::storage::projections::Projections;
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{DbError, Result};

use crate::metadata::ParquetMetaData;
use crate::metadata::loader::MetaDataLoader;
use crate::reader::Reader;
use crate::schema::convert::ColumnSchemaTypeVisitor;

pub const FUNCTION_SET_READ_PARQUET: TableFunctionSet = TableFunctionSet {
    name: "read_parquet",
    aliases: &["parquet_scan"],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read a parquet file.",
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

pub struct ReadParquetBindState {
    fs: FileSystemWithState,
    path: String,
    metadata: Arc<ParquetMetaData>,
    schema: ColumnSchema, // TODO
}

pub struct ReadParquetOperatorState {
    fs: FileSystemWithState,
    path: String,
    metadata: Arc<ParquetMetaData>,
    projections: Projections,
    filters: Vec<PhysicalScanFilter>,
    schema: ColumnSchema, // TODO
}

pub struct ReadParquetPartitionState {
    state: PartitionScanState,
}

enum PartitionScanState {
    Opening {
        open_fut: FileSystemFuture<'static, Result<Reader>>,
    },
    Scanning {
        reader: Reader,
    },
    #[allow(unused)] // Will be used when we have multiple files to scan.
    Exhausted,
}

impl TableScanFunction for ReadParquet {
    type BindState = ReadParquetBindState;
    type OperatorState = ReadParquetOperatorState;
    type PartitionState = ReadParquetPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let path = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        // TODO: You guessed it, globs
        let fs = scan_context.dispatch.filesystem_for_path(&path)?;
        let context = FileOpenContext::new(scan_context.database_context, &input.named);
        let fs = fs.try_with_context(context)?;
        match fs.stat(&path).await? {
            Some(stat) if stat.file_type.is_file() => (), // We have a file.
            Some(_) => return Err(DbError::new("Cannot read parquet from a directory")),
            None => return Err(DbError::new(format!("Missing file for path '{path}'"))),
        }

        let mut file = fs.open(OpenFlags::READ, &path).await?;
        let loader = MetaDataLoader::new();
        let metadata = loader.load_from_file(&mut file).await?;

        let schema =
            ColumnSchemaTypeVisitor.convert_schema(&metadata.file_metadata.schema_descr)?;
        let cardinality = metadata.file_metadata.num_rows as usize;

        Ok(TableFunctionBindState {
            state: ReadParquetBindState {
                fs,
                path,
                metadata: Arc::new(metadata),
                schema: schema.clone(),
            },
            input,
            schema,
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
            path: bind_state.path.clone(),
            metadata: bind_state.metadata.clone(),
            projections,
            filters: filters.to_vec(),
            schema: bind_state.schema.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut partition_row_groups: Vec<_> = (0..partitions).map(|_| Vec::new()).collect();

        for rg_idx in 0..op_state.metadata.row_groups.len() {
            let part_idx = rg_idx % partitions;
            partition_row_groups[part_idx].push(rg_idx);
        }

        let states = partition_row_groups
            .into_iter()
            .map(|groups| {
                let projections = op_state.projections.clone();
                let metadata = op_state.metadata.clone();
                let schema = op_state.schema.clone();
                let open_fut = op_state
                    .fs
                    .open_static(OpenFlags::READ, op_state.path.clone());

                let fut = Box::pin(async move {
                    let file = open_fut.await?;
                    // TODO: How do we want to thread down the manager? Put on
                    // props? Request that it goes on op_state?
                    Reader::try_new(
                        &DefaultBufferManager,
                        metadata,
                        schema,
                        file,
                        groups,
                        projections,
                        &op_state.filters,
                    )
                });

                ReadParquetPartitionState {
                    state: PartitionScanState::Opening { open_fut: fut },
                }
            })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        _op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        loop {
            match &mut state.state {
                PartitionScanState::Opening { open_fut } => {
                    let reader = match open_fut.poll_unpin(cx)? {
                        Poll::Ready(reader) => reader,
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    state.state = PartitionScanState::Scanning { reader };
                    // Continue...
                }
                PartitionScanState::Scanning { reader } => {
                    let poll = reader.poll_pull(cx, output)?;
                    // TODO: If poll == Exhausted, move to the next file open.
                    return Ok(poll);
                }
                PartitionScanState::Exhausted => {
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }
            }
        }
    }
}
