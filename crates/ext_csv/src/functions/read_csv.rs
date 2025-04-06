use std::task::{Context, Poll};

use futures::FutureExt;
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
use glaredb_core::runtime::filesystem::{AnyFile, AnyFileSystem, FileSystemFuture, OpenFlags};
use glaredb_core::storage::projections::Projections;
use glaredb_error::{DbError, Result};

use crate::decoder::{ByteRecords, CsvDecoder};
use crate::dialect::DialectOptions;
use crate::reader::CsvReader;
use crate::schema::CsvSchema;

pub const FUNCTION_SET_READ_CSV: TableFunctionSet = TableFunctionSet {
    name: "read_csv",
    aliases: &["scan_csv"],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read a csv file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &ReadCsv,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct ReadCsv;

pub struct ReadCsvBindState {
    fs: AnyFileSystem,
    path: String,
    has_header: bool,
    dialect: DialectOptions,
}

pub struct ReadCsvOperatorState {
    fs: AnyFileSystem,
    path: String,
    has_header: bool,
    projections: Projections,
    dialect: DialectOptions,
}

pub enum ReadCsvPartitionState {
    Opening {
        open_fut: FileSystemFuture<'static, Result<AnyFile>>,
    },
    Scanning {
        reader: CsvReader,
    },
    Exhausted,
}

impl TableScanFunction for ReadCsv {
    type BindState = ReadCsvBindState;
    type OperatorState = ReadCsvOperatorState;
    type PartitionState = ReadCsvPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let path = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        // TODO: Glob stuff, that's going to be common thing for all file formats.
        let fs = scan_context.dispatch.filesystem_for_path(&path)?;
        match fs.call_stat(&path).await? {
            Some(stat) if stat.file_type.is_file() => (), // We have a file.
            Some(_) => return Err(DbError::new("Cannot read lines from a directory")),
            None => return Err(DbError::new(format!("Missing file for path '{path}'"))),
        }

        // Infer.
        const INFER_BUF_SIZE: usize = 1024;
        let mut infer_buf = vec![0; INFER_BUF_SIZE];
        let mut records = ByteRecords::with_buffer_capacity(INFER_BUF_SIZE);

        let mut file = fs.call_open(OpenFlags::READ, &path).await?;
        let n = file.call_read_fill(&mut infer_buf).await?;

        let infer_buf = &infer_buf[0..n];
        let dialect =
            DialectOptions::infer_from_sample(infer_buf, &mut records).unwrap_or_default();

        records.clear_all();
        let mut decoder = CsvDecoder::new(dialect);
        let _ = decoder.decode(infer_buf, &mut records);

        let schema = CsvSchema::infer_from_records(&records)?;
        let col_schema = schema.schema;

        // TODO: We drop the file/buffer here. While that's potentially
        // wasteful, I think it actually helps structure the code in a way to
        // integrate file opening in the poll method which _should_ make
        // multi-file read easier, we just have a queue of paths lined up.
        //
        // This keeps everything async.

        Ok(TableFunctionBindState {
            state: ReadCsvBindState {
                fs: fs.clone(),
                path,
                has_header: schema.has_header,
                dialect,
            },
            input,
            schema: col_schema,
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        // TODO: Seems like a common pattern that operator state is always a
        // superset (or close to it) of bind state.
        Ok(ReadCsvOperatorState {
            fs: bind_state.fs.clone(),
            path: bind_state.path.clone(),
            has_header: bind_state.has_header,
            projections,
            dialect: bind_state.dialect,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        // Single partition reads for now.
        let mut states = vec![ReadCsvPartitionState::Opening {
            open_fut: op_state
                .fs
                .call_open_static(OpenFlags::READ, op_state.path.clone()),
        }];

        states.resize_with(partitions, || ReadCsvPartitionState::Exhausted);

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        loop {
            match state {
                ReadCsvPartitionState::Opening { open_fut } => {
                    let file = match open_fut.poll_unpin(cx) {
                        Poll::Ready(Ok(file)) => file,
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    // TODO: Arbitrary and untracked.
                    const READ_BUF_SIZE: usize = 4096;
                    let read_buf = vec![0; READ_BUF_SIZE];

                    let decoder = CsvDecoder::new(op_state.dialect);
                    // TODO: I don't remember why this provided separately. Why
                    // do we need read buf and this?
                    let records = ByteRecords::with_buffer_capacity(READ_BUF_SIZE);

                    let reader = CsvReader::new(
                        file,
                        op_state.has_header,
                        op_state.projections.clone(),
                        read_buf,
                        decoder,
                        records,
                    );

                    *state = ReadCsvPartitionState::Scanning { reader };
                    continue;
                }
                ReadCsvPartitionState::Scanning { reader } => {
                    let poll = reader.poll_pull(cx, output)?;
                    if poll == PollPull::Exhausted {
                        // TODO: This is where we'd flip back to the opening
                        // state with the next file in the queue and continue
                        // the loop.
                        *state = ReadCsvPartitionState::Exhausted;
                        return Ok(PollPull::Exhausted);
                    }
                    return Ok(poll);
                }
                ReadCsvPartitionState::Exhausted => {
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }
            }
        }
    }
}
