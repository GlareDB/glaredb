use std::collections::VecDeque;
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
use glaredb_core::runtime::filesystem::file_provider::{MultiFileData, MultiFileProvider};
use glaredb_core::runtime::filesystem::{
    AnyFile,
    FileSystemFuture,
    FileSystemWithState,
    OpenFlags,
};
use glaredb_core::statistics::value::StatisticsValue;
use glaredb_core::storage::projections::Projections;
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{DbError, Result};

use crate::decoder::{ByteRecords, CsvDecoder};
use crate::dialect::DialectOptions;
use crate::reader::{CsvReader, CsvShape};
use crate::schema::CsvSchema;

pub const FUNCTION_SET_READ_CSV: TableFunctionSet = TableFunctionSet {
    name: FnName::default("read_csv"),
    aliases: &[FnName::default("scan_csv")],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read a csv file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ReadCsv,
        ),
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::List], DataTypeId::Table),
            &ReadCsv,
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct ReadCsv;

pub struct ReadCsvBindState {
    fs: FileSystemWithState,
    mf_data: MultiFileData,
    shape: CsvShape,
    dialect: DialectOptions,
}

pub struct ReadCsvOperatorState {
    fs: FileSystemWithState,
    mf_data: MultiFileData,
    shape: CsvShape,
    projections: Projections,
    dialect: DialectOptions,
}

pub struct ReadCsvPartitionState {
    /// Current read state.
    state: ReadState,
    /// Queue of files this partition will be handling.
    queue: VecDeque<String>,
    /// Reader that's reused across all files.
    reader: Box<CsvReader>,
}

enum ReadState {
    /// Initialize the next file to read.
    Init,
    /// Currently opening a file.
    Opening {
        open_fut: FileSystemFuture<'static, Result<AnyFile>>,
    },
    /// Currently scanning a file.
    Scanning,
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
        let (mut provider, fs) =
            MultiFileProvider::try_new_from_inputs(scan_context, &input).await?;

        let mut mf_data = MultiFileData::empty();
        provider.expand_all(&mut mf_data).await?;

        // Use first file for schema inference.
        //
        // TODO: Infer using multiple files.
        let first = mf_data
            .get(0)
            .ok_or_else(|| DbError::new("No files for path, expected at least one file"))?;

        // Infer.
        const INFER_BUF_SIZE: usize = 4096; // TODO: Have a reason for this size. Currently just "gut feeling".
        let mut infer_buf = vec![0; INFER_BUF_SIZE];
        let mut records = ByteRecords::with_buffer_capacity(INFER_BUF_SIZE);

        let mut file = fs.open(OpenFlags::READ, first).await?;
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
                mf_data,
                shape: CsvShape {
                    has_header: schema.has_header,
                    num_columns: col_schema.fields.len(),
                },
                dialect,
            },
            input,
            data_schema: col_schema,
            meta_schema: Some(provider.meta_schema()),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        // TODO: Seems like a common pattern that operator state is always a
        // superset (or close to it) of bind state.
        Ok(ReadCsvOperatorState {
            fs: bind_state.fs.clone(),
            mf_data: bind_state.mf_data.clone(), // TODO
            shape: bind_state.shape,
            projections,
            dialect: bind_state.dialect,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let expanded = op_state.mf_data.expanded();

        // Split files to read across all partitions.
        let states = (0..partitions)
            .map(|partition_idx| {
                let queue: VecDeque<_> = expanded
                    .iter()
                    .skip(partition_idx)
                    .step_by(partitions)
                    .map(|path| path.to_string())
                    .collect();

                // TODO: Arbitrary and untracked.
                const READ_BUF_SIZE: usize = 1024 * 1024 * 4; // 4MB
                let read_buf = vec![0; READ_BUF_SIZE];

                let decoder = CsvDecoder::new(op_state.dialect);
                // TODO: I don't remember why this provided separately. Why
                // do we need read buf and this?
                let records = ByteRecords::with_buffer_capacity(READ_BUF_SIZE);

                let reader = CsvReader::new(
                    op_state.shape,
                    op_state.projections.clone(),
                    read_buf,
                    decoder,
                    records,
                );

                ReadCsvPartitionState {
                    state: ReadState::Init,
                    queue,
                    reader: Box::new(reader),
                }
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
            match &mut state.state {
                ReadState::Init => {
                    let path = match state.queue.pop_front() {
                        Some(path) => path,
                        None => {
                            // We're done.
                            output.set_num_rows(0)?;
                            return Ok(PollPull::Exhausted);
                        }
                    };

                    let open_fut = op_state.fs.open_static(OpenFlags::READ, path);
                    state.state = ReadState::Opening { open_fut };
                    // Continue...
                }
                ReadState::Opening { open_fut } => {
                    let file = match open_fut.poll_unpin(cx) {
                        Poll::Ready(Ok(file)) => file,
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    state.reader.prepare(file);
                    state.state = ReadState::Scanning;
                    // Continue...
                }
                ReadState::Scanning => {
                    let poll = state.reader.poll_pull(cx, output)?;
                    if poll == PollPull::Exhausted {
                        // Flip back to init to read the next path from the queue.
                        state.state = ReadState::Init;
                        continue;
                    }
                    return Ok(poll);
                }
            }
        }
    }
}
