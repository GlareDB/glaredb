use catalog_types::keys::PartitionKey;
use datafusion::arrow::record_batch::RecordBatch;
use scc::HashMap;
use tracing::trace;

/// In-memory cache of changes made to underlying table partitions.
///
/// A single delta cache handles deltas for all tables in the system.
#[derive(Debug)]
pub struct DeltaCache {
    inserts: HashMap<PartitionKey, Vec<RecordBatch>>,
}

impl DeltaCache {
    pub fn new() -> DeltaCache {
        DeltaCache {
            inserts: HashMap::new(),
        }
    }

    /// Insert a batch for a partition.
    pub fn insert_batch(&self, part: &PartitionKey, batch: RecordBatch) {
        trace!(%part, "inserting for partition");
        // TODO: Remove needing to clone the batch before insert.
        let cloned = batch.clone();
        self.inserts.upsert(
            part.clone(),
            || vec![cloned],
            |_, batches| batches.push(batch),
        )
    }

    pub fn partition_inserts(&self, part: &PartitionKey) -> Vec<RecordBatch> {
        trace!(%part, "reading partition inserts");
        self.inserts
            .read(part, |_, batches| batches.clone())
            .unwrap_or_default()
    }

    pub fn remove_partition_deltas(&self, part: &PartitionKey) {
        let _ = self.inserts.remove(part);
    }
}

impl Default for DeltaCache {
    fn default() -> Self {
        Self::new()
    }
}
