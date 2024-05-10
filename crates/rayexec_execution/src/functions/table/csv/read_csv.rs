use crate::functions::table::{
    BoundTableFunctionOld, Pushdown, Statistics, TableFunctionArgs, TableFunctionOld,
};
use crate::physical::plans::{PollPull, SourceOperator2};
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use parking_lot::Mutex;
use rayexec_bullet::csv::reader::{CsvSchema, DialectOptions, TypedDecoder};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy)]
pub struct ReadCsv;

impl TableFunctionOld for ReadCsv {
    fn name(&self) -> &str {
        "read_csv"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunctionOld>> {
        // TODO: Named arg stuff
        // TODO: Dispatch on object store functions if not a local path.
        // TODO: Globs
        if args.unnamed.len() != 1 {
            return Err(RayexecError::new("Invalid number of arguments"));
        }

        let path = args.unnamed.first().unwrap().try_as_str()?;
        let path = Path::new(path);

        // TODO: Pass me in
        const INFER_COUNT: usize = 2048;
        let bound = ReadCsvLocal::new_from_path(path, INFER_COUNT, 8192)?;

        Ok(Box::new(bound))
    }
}

/// Represents reading a single csv file from local disk.
struct ReadCsvLocal {
    path: String, // Only for explain.

    /// Desired batch size.
    batch_size: usize,

    /// Schema of the file.
    schema: Schema,

    /// Reader responsible for emitting typed arrays from records.
    reader: TypedDecoder,

    /// Underlying file.
    file: BufReader<fs::File>,
}

impl ReadCsvLocal {
    fn new_from_path(
        path: impl AsRef<Path>,
        infer_count: usize,
        batch_size: usize,
    ) -> Result<Self> {
        let file = fs::OpenOptions::new().read(true).open(&path)?;
        let mut buf_reader = BufReader::new(file);

        // TODO: I think there can be helpers in the csv module to do all this
        // for us. I just didn't want to get locked into an abstraction yet.

        // Determine dialect using a small sample from the file.
        //
        // If dialect cannot be inferred, just use the default.
        let mut sample_buf = vec![0; 1024];
        let sample_read = buf_reader.read(&mut sample_buf)?;
        let dialect =
            DialectOptions::infer_from_sample(&sample_buf[..sample_read]).unwrap_or_default();

        // Reset to beginning.
        buf_reader
            .seek(SeekFrom::Start(0))
            .context("Failed to seek")?;

        // Now try to infer schema based on some number of records.
        let mut decoder = dialect.decoder();
        let mut count = 0;
        loop {
            let buf = buf_reader.fill_buf()?;
            let result = decoder.decode(&buf)?;
            buf_reader.consume(result.input_offset);
            count += result.completed;

            if result.completed == 0 || count >= infer_count {
                break;
            }
        }

        let records = decoder.flush()?;
        let csv_schema = CsvSchema::infer_from_records(records)?;

        // Build the actual reader from the inferred schema/dialect.
        let reader = TypedDecoder::new(dialect, &csv_schema);

        // Reset file. Note this does throw away the work we did decoding the
        // records above.
        buf_reader.seek(SeekFrom::Start(0))?;

        Ok(ReadCsvLocal {
            path: path.as_ref().to_string_lossy().to_string(),
            schema: csv_schema.into_schema(),
            reader,
            file: buf_reader,
            batch_size,
        })
    }
}

impl fmt::Debug for ReadCsvLocal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCsvLocal")
            .field("path", &self.path)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl BoundTableFunctionOld for ReadCsvLocal {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            estimated_cardinality: None,
            max_cardinality: None,
        }
    }

    fn into_source(
        self: Box<Self>,
        _projection: Vec<usize>,
        _pushdown: Pushdown,
    ) -> Result<Box<dyn SourceOperator2>> {
        unimplemented!()
        // Ok(Box::new(ReadCsvLocalOperator {
        //     path: self.path,
        //     reader: Mutex::new(self.reader),
        // }))
    }
}

impl Explainable for ReadCsvLocal {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(ReadCsv.name()).with_value("path", &self.path)
    }
}

/// State holding the file and reader.
#[derive(Debug)]
struct ReaderState {
    batch_size: usize,
    reader: TypedDecoder,
    file: BufReader<fs::File>,
}

// impl ReaderState {
//     fn try_read
// }

#[derive(Debug)]
struct ReadCsvLocalOperator {
    path: String, // Only for explain.
    reader: Mutex<ReaderState>,
}

impl SourceOperator2 for ReadCsvLocalOperator {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_pull(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context<'_>,
        partition: usize,
    ) -> Result<PollPull> {
        assert_eq!(0, partition);
        unimplemented!()
        // let mut reader = self.reader.lock();
        // match reader.next() {
        //     Some(Ok(batch)) => Ok(PollPull::Batch(DataBatch::from(batch))),
        //     Some(Err(e)) => Err(e.into()),
        //     None => Ok(PollPull::Exhausted),
        // }
    }
}

impl Explainable for ReadCsvLocalOperator {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(ReadCsv.name()).with_value("path", &self.path)
    }
}
