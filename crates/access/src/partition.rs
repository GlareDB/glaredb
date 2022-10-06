use crate::deltacache::DeltaCache;
use crate::errors::{internal, Result};
use crate::memcache::MemCache;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Expr;
use std::sync::Arc;

pub struct Partition {
    /// /schema_1/table_1_part_<part_id>.data
    part_id: u32,
    cache: Arc<MemCache>,
    deltas: Arc<DeltaCache>,
}

impl Partition {
    pub async fn get_record_batch(&self, batch_id: u32) -> Result<RecordBatch> {
        // Async due to possibly fetching from object storage.
        unimplemented!()
    }

    // TODO: How to refence records in recently inserted batch?
    pub async fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        unimplemented!()
    }

    pub async fn scan_partition(
        &self,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<PartitionStream> {
        unimplemented!()
    }

    pub fn scan_delta(
        &self,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<DeltaStream> {
        unimplemented!()
    }
}

pub struct PartitionStream {} // futures stream

pub struct DeltaStream {
    batches: Vec<RecordBatch>,
} // futures stream

pub struct CombinedPartitionStream {} // futures stream

pub struct Table {
    // references all partitions
}

impl Table {
    pub async fn scan(
        &self,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<TableStream> {
        // Chain all partition stream one after another.
        unimplemented!()
    }
}

pub struct TableStream {} // futures stream
