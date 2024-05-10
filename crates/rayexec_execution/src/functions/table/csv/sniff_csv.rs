use crate::functions::table::{
    BoundTableFunctionOld, Pushdown, Statistics, TableFunctionArgs, TableFunctionOld,
};
use crate::physical::plans::{PollPull, SourceOperator2};
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use parking_lot::Mutex;
use rayexec_bullet::array::{Array, BooleanArray, Utf8Array};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::csv::reader::{CsvSchema, DialectOptions, TypedDecoder};
use rayexec_bullet::field::{DataType, Field, Schema};
use rayexec_error::{RayexecError, Result, ResultExt};
use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};

/// Number of bytes to read to try to infer the csv dialect.
const DEFAULT_DIALECT_NUM_BYTES: usize = 1024;

/// Default number of records to read to try to infer the csv schema.
const DEFAULT_RECORD_INFER_SIZE: usize = 2048;

#[derive(Debug, Clone, Copy)]
pub struct SniffCsv;

impl TableFunctionOld for SniffCsv {
    fn name(&self) -> &str {
        "sniff_csv"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunctionOld>> {
        if args.unnamed.len() != 1 {
            return Err(RayexecError::new("Invalid number of arguments"));
        }

        let path = args.unnamed.first().unwrap().try_as_str()?;
        let path = Path::new(path);

        Ok(Box::new(SniffCsvLocal::new(
            path,
            DEFAULT_DIALECT_NUM_BYTES,
            DEFAULT_RECORD_INFER_SIZE,
        )?))
    }
}

#[derive(Debug)]
struct SniffCsvLocal {
    /// Number of bytes to read to infer the dialect.
    dialect_bytes: usize,

    /// Number of records to read to infer the schema (types, header).
    infer_count: usize,

    /// File to read from.
    file: BufReader<fs::File>,

    /// Output schema.
    schema: Schema,

    finished: bool,
}

impl SniffCsvLocal {
    fn new(path: impl AsRef<Path>, dialect_bytes: usize, infer_count: usize) -> Result<Self> {
        let file = fs::OpenOptions::new().read(true).open(&path)?;
        let schema = Schema::new([
            Field::new("delimiter", DataType::Utf8, false),
            Field::new("quote", DataType::Utf8, false),
            Field::new("has_header", DataType::Boolean, false),
            Field::new("columns", DataType::Utf8, false),
        ]);
        Ok(SniffCsvLocal {
            dialect_bytes,
            infer_count,
            file: BufReader::new(file),
            schema,
            finished: false,
        })
    }
}

impl BoundTableFunctionOld for SniffCsvLocal {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            estimated_cardinality: Some(1),
            max_cardinality: Some(1),
        }
    }

    fn into_source(
        self: Box<Self>,
        _projection: Vec<usize>,
        _pushdown: Pushdown,
    ) -> Result<Box<dyn SourceOperator2>> {
        Ok(Box::new(SniffCsvLocalSource {
            inner: Mutex::new(*self),
        }))
    }
}

impl Explainable for SniffCsvLocal {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(SniffCsv.name())
    }
}

#[derive(Debug)]
struct SniffCsvLocalSource {
    inner: Mutex<SniffCsvLocal>,
}

impl SourceOperator2 for SniffCsvLocalSource {
    fn output_partitions(&self) -> usize {
        1
    }

    fn poll_pull(
        &self,
        _task_cx: &TaskContext,
        _cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull> {
        assert_eq!(0, partition);

        let mut inner = self.inner.lock();

        if inner.finished {
            return Ok(PollPull::Exhausted);
        }

        // Read a sample.
        let mut sample_buf = vec![0; DEFAULT_DIALECT_NUM_BYTES];
        let sample_read = inner.file.read(&mut sample_buf)?;
        let dialect =
            DialectOptions::infer_from_sample(&sample_buf[..sample_read]).unwrap_or_default();

        inner.file.seek(SeekFrom::Start(0))?;

        println!("--- inferred");

        // Now try to infer schema based on some number of records.
        let mut decoder = dialect.decoder();
        let n = inner.file.read(&mut sample_buf)?;
        decoder.decode(&sample_buf[..n])?;

        let records = decoder.flush()?;
        let csv_schema = CsvSchema::infer_from_records(records)?;

        // 'column1: Type, column2: Type, ...'
        let output_schema_str = csv_schema
            .fields
            .iter()
            .map(|field| format!("{}: {}", field.name, field.datatype))
            .collect::<Vec<_>>()
            .join(", ");

        let cols = [
            // delimiter
            Array::Utf8(Utf8Array::from_iter([String::from_utf8(vec![
                dialect.delimiter,
            ])
            .unwrap()
            .as_str()])),
            // quote
            Array::Utf8(Utf8Array::from_iter([String::from_utf8(vec![
                dialect.quote,
            ])
            .unwrap()
            .as_str()])),
            // has header
            Array::Boolean(BooleanArray::from_iter([csv_schema.has_header])),
            // columns
            Array::Utf8(Utf8Array::from_iter([output_schema_str.as_str()])),
        ];

        inner.finished = true;

        Ok(PollPull::Batch(
            Batch::try_new(cols).expect("batch to be valid"),
        ))
    }
}

impl Explainable for SniffCsvLocalSource {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}
