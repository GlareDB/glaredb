use std::{
    collections::VecDeque,
    fmt::{self, Debug},
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{array::AsyncBatchReader, metadata::Metadata};
use futures::{stream::BoxStream, StreamExt};
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::Result;
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    execution::operators::PollPull,
};
use rayexec_io::AsyncReader;

/// Helper trait for generating a reader per partition.
pub trait ReaderBuilder<R: AsyncReader>: Sync + Send + Debug {
    /// Creates a new reader for reading a parquet file.
    fn new_reader(&self) -> Result<R>;
}

/// Data table implementation which parallelizes on row groups. During scanning,
/// each returned scan object is responsible for distinct row groups to read.
#[derive(Debug)]
pub struct RowGroupPartitionedDataTable<B, R> {
    reader_builder: B,
    metadata: Arc<Metadata>,
    schema: Schema,

    _reader: PhantomData<R>,
}

impl<B, R> RowGroupPartitionedDataTable<B, R>
where
    B: ReaderBuilder<R>,
    R: AsyncReader + 'static,
{
    pub fn new(reader_builder: B, metadata: Arc<Metadata>, schema: Schema) -> Self {
        RowGroupPartitionedDataTable {
            reader_builder,
            metadata,
            schema,
            _reader: PhantomData,
        }
    }
}

impl<B, R> DataTable for RowGroupPartitionedDataTable<B, R>
where
    B: ReaderBuilder<R>,
    R: AsyncReader + 'static,
{
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut partitioned_row_groups = vec![VecDeque::new(); num_partitions];

        // Split row groups into individual partitions.
        for row_group in 0..self.metadata.parquet_metadata.row_groups().len() {
            let partition = row_group % num_partitions;
            partitioned_row_groups[partition].push_back(row_group);
        }

        let readers = partitioned_row_groups
            .into_iter()
            .map(|row_groups| {
                let reader = self.reader_builder.new_reader()?;
                const BATCH_SIZE: usize = 2048; // TODO
                AsyncBatchReader::try_new(
                    reader,
                    row_groups,
                    self.metadata.clone(),
                    &self.schema,
                    BATCH_SIZE,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let scans: Vec<Box<dyn DataTableScan>> = readers
            .into_iter()
            .map(|reader| {
                Box::new(RowGroupsScan {
                    stream: reader.into_stream(),
                }) as _
            })
            .collect();

        Ok(scans)
    }
}

struct RowGroupsScan {
    stream: BoxStream<'static, Result<Batch>>,
}

impl DataTableScan for RowGroupsScan {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Ok(PollPull::Batch(batch)),
            Poll::Ready(Some(Err(e))) => {
                println!("ERR: {e}");
                Err(e)
            }
            Poll::Ready(None) => Ok(PollPull::Exhausted),
            Poll::Pending => Ok(PollPull::Pending),
        }
    }
}

impl fmt::Debug for RowGroupsScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowGroupsScan").finish_non_exhaustive()
    }
}
