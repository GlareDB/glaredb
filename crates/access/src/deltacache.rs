use crate::errors::{internal, Result};
use crate::keys::PartitionKey;
use datafusion::arrow::record_batch::RecordBatch;
use scc::HashMap;

/// An in-memory cache of modifications to partitions.
///
/// Provides a global cache for all tables.
#[derive(Debug)]
pub struct DeltaCache {
    inserts: HashMap<PartitionKey, Vec<RecordBatch>>,
}

impl DeltaCache {
    pub fn insert_batch_for_part(&self, key: &PartitionKey, batch: RecordBatch) -> Result<()> {
        // TODO: Remove needing to clone the batch when upserting.
        self.inserts.upsert(
            key.clone(),
            || vec![batch.clone()],
            |_, batches| batches.push(batch.clone()),
        );
        Ok(())
    }

    pub fn clone_batches_for_part(&self, key: &PartitionKey) -> Result<Vec<RecordBatch>> {
        let batches = self
            .inserts
            .read(key, |_, batches| batches.clone())
            .unwrap_or(Vec::new());
        Ok(batches)
    }
}
