use crate::errors::Result;
use datafusion::arrow::record_batch::RecordBatch;
use persistence::file::DiskCache;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Hash)]
pub struct BatchKey {
    pub part_id: u32,
    pub batch: u32,
}

#[derive(Debug)]
pub struct MemCache {
    cache: HashMap<BatchKey, RecordBatch>,
    disk: Arc<DiskCache>,
}

impl MemCache {
    pub async fn get_record_batch(&self, key: &BatchKey) -> Result<RecordBatch> {
        todo!()
    }

    async fn evict(&self) -> Result<()> {
        todo!()
    }
}
