use crate::deltacache::DeltaCache;
use crate::errors::{Result};
use crate::memcache::MemCache;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Expr;
use std::sync::Arc;

/// A partition contains a range of records for a table.
///
/// A range of records will be sorted by some arbitrary sort key determined at a
/// higher level.
pub struct Partition {
    /// The partition id for a table. The id is unique amongst all partitions
    /// for a table. Partition ids do not indicate relative order of partitions.
    /// A higher level index structure is needed to determine the order of
    /// partitions.
    ///
    /// The partition id can be derived from the the file name:
    /// e.g. `/schema_1/table_1_part_<part_id>.data`
    part_id: u32,
    cache: Arc<MemCache>,
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

    pub async fn scan_partition(
        &self,
        _projection: Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<PartitionStream> {
        unimplemented!()
    }

    pub fn scan_delta(
        &self,
        _projection: Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<DeltaStream> {
        unimplemented!()
    }
}

// TODO: futures stream
pub struct PartitionStream {}

// TODO: futures stream
pub struct DeltaStream {
    batches: Vec<RecordBatch>,
}

// TODO: futures stream
pub struct CombinedPartitionStream {}
