use crate::delta::TableDelta;
use crate::errors::{internal, ExpstoreError, Result};
use crate::file::{LocalCache, MirroredFile};
use crate::format::data::DataReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

// TODO: This is pretty much storing the table as a single partition. When we
// have the meta file ready, we can start looking at splitting into multiple
// partitions.
pub struct Table {
    delta: TableDelta,
    partition: PartitionFile,
}

impl Table {
    pub fn insert_bulk(&self, batch: RecordBatch) -> Result<()> {
        self.delta.insert_batch(batch)
    }

    pub async fn scan(
        &self,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<TableScanStream<'_>> {
        unimplemented!()
    }
}

pub struct TableScanStream<'a> {
    table: &'a Table,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl<'a> TableScanStream<'a> {
    fn next(&self) -> Result<Option<RecordBatch>> {
        unimplemented!()
    }
}

// TODO: Track memory used by cached batches.
struct PartitionFile {
    cache: Arc<LocalCache>,
    path: PathBuf,
    /// The underlying wrapped file. Note that this is wrapped in a mutex due to
    /// the existing implementation of reading a batch at some index.
    reader: Mutex<Option<DataReader>>,
    /// Batches that we've already read from the underlying file.
    batches: RwLock<BTreeMap<usize, RecordBatch>>,
}

impl PartitionFile {
    async fn read_batch_at(&self, idx: usize) -> Result<RecordBatch> {
        {
            let batches = self.batches.read();
            if let Some(batch) = batches.get(&idx) {
                return Ok(batch.clone());
            }
        }

        let mut reader = self.reader.lock();
        let batch = match reader.as_mut() {
            Some(reader) => reader.read_batch_at(idx)?,
            None => {
                let mirrored =
                    MirroredFile::open_from_remote(&self.path, self.cache.clone()).await?;
                let mut data = DataReader::with_file(mirrored)?;
                let batch = data.read_batch_at(idx)?;
                *reader = Some(data);
                batch
            }
        };

        let mut batches = self.batches.write();
        batches.insert(idx, batch.clone());

        Ok(batch)
    }
}
