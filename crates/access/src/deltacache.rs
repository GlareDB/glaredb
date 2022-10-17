use crate::errors::Result;
use crate::keys::PartitionKey;
use crate::modify::{StreamModifier, StreamModifierOpener};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{memory::MemoryStream, SendableRecordBatchStream};
use futures::future::BoxFuture;
use scc::HashMap;

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
        // TODO: Remove needing to clone the batch before insert.
        let cloned = batch.clone();
        self.inserts.upsert(
            part.clone(),
            || vec![cloned],
            |_, batches| batches.push(batch),
        )
    }

    pub fn remove_partition_deltas(&self, part: &PartitionKey) {
        let _ = self.inserts.remove(part);
    }
}

impl StreamModifierOpener<PartitionDeltaModifier> for DeltaCache {
    fn open_modifier(
        &self,
        partition: &PartitionKey,
        schema: &SchemaRef,
    ) -> Result<BoxFuture<'static, PartitionDeltaModifier>> {
        let batches = self
            .inserts
            .read(partition, |_, batches| batches.clone())
            .unwrap_or_default();
        let partition = partition.clone();
        let schema = schema.clone();
        Ok(Box::pin(async move {
            PartitionDeltaModifier {
                partition,
                schema,
                inserts: batches,
            }
        }))
    }
}

impl Default for DeltaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Modify a partition stream with a partition's deltas.
#[derive(Debug)]
pub struct PartitionDeltaModifier {
    pub(crate) partition: PartitionKey,
    pub(crate) schema: SchemaRef,
    pub(crate) inserts: Vec<RecordBatch>,
}

impl StreamModifier for PartitionDeltaModifier {
    fn modify(&self, batch: RecordBatch) -> Result<RecordBatch> {
        Ok(batch) // No modifications to make yet.
    }

    fn stream_rest(&self) -> SendableRecordBatchStream {
        let stream =
            MemoryStream::try_new(self.inserts.clone(), self.schema.clone(), None).unwrap(); // Doesn't error.
        Box::pin(stream)
    }
}
