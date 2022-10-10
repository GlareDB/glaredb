use crate::deltacache::DeltaCache;
use crate::errors::Result;
use crate::keys::PartitionKey;
use crate::memcache::MemCache;
use crate::partitionexec::{StreamOpenFuture, StreamOpener};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Expr;
use futures::{future::BoxFuture, ready, stream::BoxStream, FutureExt, Stream, StreamExt};
use persistence::file::{DiskCache, MirroredFile};
use std::sync::Arc;

/// A partition contains a range of records for a table.
///
/// A range of records will be sorted by some arbitrary sort key determined at a
/// higher level.
pub struct Partition {
    part: PartitionKey,
    cache: Arc<MemCache>,
    disk: Arc<DiskCache>,
    deltas: Arc<DeltaCache>,
}

impl Partition {
    pub async fn get_record_batch(&self, _batch_id: u32) -> Result<RecordBatch> {
        // Async due to possibly fetching from object storage.
        unimplemented!()
    }

    // TODO: How to refence records in recently inserted batch?
    pub async fn insert_batch(&self, _batch: RecordBatch) -> Result<()> {
        unimplemented!()
    }
}

pub type PartitionReaderStreamFuture = BoxFuture<'static, BoxStream<'static, Result<RecordBatch>>>;

pub trait PartitionReader: Clone + Send + Sync {
    fn read_partition(&self) -> Result<PartitionReaderStreamFuture>;
}

pub type PartitionWriterFuture = BoxFuture<'static, Result<()>>;

pub trait PartitionWriter: Clone + Send + Sync {
    fn write_partition(&self) -> Result<PartitionWriterFuture>;
}

/// Read a partition from the local node.
#[derive(Debug, Clone)]
pub struct LocalPartitionReader {
    part: PartitionKey,
    disk: Arc<DiskCache>,
    mem: Arc<MemCache>,
    deltas: Arc<DeltaCache>,
}

impl PartitionReader for LocalPartitionReader {
    fn read_partition(&self) -> Result<PartitionReaderStreamFuture> {
        unimplemented!()
    }
}
