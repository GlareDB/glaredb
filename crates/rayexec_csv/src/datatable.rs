use std::task::Context;
use std::{
    fmt::{self, Debug},
    task::Poll,
};

use futures::stream::{BoxStream, StreamExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use rayexec_execution::database::table::EmptyTableScan;
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    execution::operators::PollPull,
};
use rayexec_io::AsyncReader;

use crate::reader::{AsyncCsvReader, CsvSchema, DialectOptions};

// TODO: This is a common trait between parquet/csv. I think there's a way to
// unify these somehow to have it more integrated into the data source
// interface.
//
// Currently we need to implement this for file sources and http sources which
// isn't amazing.
//
// Also we're having to get the reader twice, once when getting the
// schema/metadata, and another we build the actual scan. This is mostly due to
// mutability limitations right now, and might be solved by just having the
// `DataTable` interface table mutable references to allow for reader reuse.
pub trait ReaderBuilder: Sync + Send + Debug {
    fn new_reader(&self) -> Result<Box<dyn AsyncReader>>;
}

/// Data table implementation that reads from a single file.
///
/// This will produce a single scan that reads the actual file, with the
/// remaining scans being empty.
///
/// This should be extended to support multiple files once we add in glob
/// support.
#[derive(Debug)]
pub struct SingleFileCsvDataTable {
    pub options: DialectOptions,
    pub csv_schema: CsvSchema,
    pub reader_builder: Box<dyn ReaderBuilder>,
}

impl DataTable for SingleFileCsvDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let reader = self.reader_builder.new_reader()?;
        let csv_reader = AsyncCsvReader::new(reader, self.csv_schema.clone(), self.options);
        let stream = csv_reader.into_stream().boxed();

        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(CsvFileScan { stream })];
        // Reset are empty (for now)
        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

pub struct CsvFileScan {
    stream: BoxStream<'static, Result<Batch>>,
}

// TODO: We could just implement `DataTableScan` for boxed streams.
impl DataTableScan for CsvFileScan {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Ok(PollPull::Batch(batch)),
            Poll::Ready(Some(Err(e))) => Err(e),
            Poll::Ready(None) => Ok(PollPull::Exhausted),
            Poll::Pending => Ok(PollPull::Pending),
        }
    }
}

impl fmt::Debug for CsvFileScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvFileScan").finish_non_exhaustive()
    }
}
