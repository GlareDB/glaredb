use crate::errors::Result;
use crate::keys::PartitionKey;
use datafusion::arrow::record_batch::RecordBatch;
use scc::HashMap;

/// An in-memory cache of modifications to partitions.
///
/// Provides a global cache for all tables.
pub struct DeltaCache {
    inserts: HashMap<PartitionKey, Vec<RecordBatch>>,
}

impl DeltaCache {
    pub fn insert_batch_for_part(&self, _key: &PartitionKey, _batch: RecordBatch) -> Result<()> {
        unimplemented!()
    }

    pub fn clone_batches_for_part(&self, _key: &PartitionKey) -> Result<Vec<RecordBatch>> {
        unimplemented!()
    }
}
