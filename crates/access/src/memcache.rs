use crate::errors::Result;
use crate::keys::BatchKey;
use datafusion::arrow::record_batch::RecordBatch;
use persistence::file::DiskCache;
use scc::HashMap;
use std::sync::Arc;

/// An in-memory cache of record batches.
///
/// This provides a global cache across all tables.
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
