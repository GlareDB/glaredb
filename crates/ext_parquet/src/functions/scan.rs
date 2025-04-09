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
use glaredb_core::logical::statistics::StatisticsValue;
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::runtime::filesystem::{FileOpenContext, FileSystemWithState, OpenFlags};
use glaredb_core::storage::projections::Projections;
use glaredb_error::{DbError, Result, not_implemented};

use crate::metadata::loader::MetaDataLoader;
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
}

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
            state: ReadParquetBindState { fs, path },
            input,
            schema,
            cardinality: StatisticsValue::Exact(cardinality),
        })
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
