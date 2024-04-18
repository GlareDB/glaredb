use crate::expr::scalar::ScalarValue;
use crate::physical::plans::{PollPull, Source};
use crate::physical::TaskContext;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{DataBatch, NamedDataBatchSchema};
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result, ResultExt};
use std::fmt;
use std::fs;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{BoundTableFunction, Statistics, TableFunction, TableFunctionArgs};

#[derive(Debug, Clone, Copy)]
pub struct ReadCsv;

impl TableFunction for ReadCsv {
    fn name(&self) -> &str {
        "read_csv"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunction>> {
        // TODO: Named arg stuff
        // TODO: Dispatch on object store functions if not a local path.
        // TODO: Globs
        if args.unnamed.len() != 1 {
            return Err(RayexecError::new("Invalid number of arguments"));
        }

        let path = match args.unnamed.first().unwrap() {
            ScalarValue::Utf8(path) => path,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected string argument, got {other:?}"
                )))
            }
        };
        let path = Path::new(path);

        const INFER_COUNT: usize = 2048;
        let bound = ReadCsvLocal::new_from_path(path, INFER_COUNT)?;

        Ok(Box::new(bound))
    }
}

/// Represents reading a single csv file from local disk.
struct ReadCsvLocal {
    path: String, // Only for explain.
    schema: NamedDataBatchSchema,
    reader: arrow::csv::reader::BufReader<BufReader<fs::File>>, // k
}

impl ReadCsvLocal {
    fn new_from_path(path: impl AsRef<Path>, infer_count: usize) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .context("Failed to open file")?;
        let mut buf_reader = BufReader::new(file);

        let format = arrow::csv::reader::Format::default().with_header(true);
        let (schema, _) = format.infer_schema(&mut buf_reader, Some(infer_count))?;
        let batch_schema = NamedDataBatchSchema::from(&schema);

        buf_reader
            .seek(SeekFrom::Start(0))
            .context("Failed to seek")?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build_buffered(buf_reader)?;

        Ok(ReadCsvLocal {
            path: path.as_ref().to_string_lossy().to_string(),
            schema: batch_schema,
            reader,
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

impl BoundTableFunction for ReadCsvLocal {
    fn schema(&self) -> NamedDataBatchSchema {
        self.schema.clone()
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
        _pushdown: super::Pushdown,
    ) -> Result<Box<dyn Source>> {
        Ok(Box::new(ReadCsvLocalOperator {
            path: self.path,
            reader: Mutex::new(self.reader),
        }))
    }
}

impl Explainable for ReadCsvLocal {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(ReadCsv.name()).with_value("path", &self.path)
    }
}

#[derive(Debug)]
struct ReadCsvLocalOperator {
    path: String, // Only for explain.
    reader: Mutex<arrow::csv::reader::BufReader<BufReader<fs::File>>>,
}

impl Source for ReadCsvLocalOperator {
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
        let mut reader = self.reader.lock();
        match reader.next() {
            Some(Ok(batch)) => Ok(PollPull::Batch(DataBatch::from(batch))),
            Some(Err(e)) => Err(e.into()),
            None => Ok(PollPull::Exhausted),
        }
    }
}

impl Explainable for ReadCsvLocalOperator {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(ReadCsv.name()).with_value("path", &self.path)
    }
}
