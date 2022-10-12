use crate::deltaexec::{BatchModifier, BatchModifierOpener};
use crate::errors::Result;
use crate::keys::PartitionKey;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{memory::MemoryStream, SendableRecordBatchStream};
use futures::future::BoxFuture;
use scc::HashMap;
use std::sync::Arc;

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
        self.inserts.upsert(
            part.clone(),
            || vec![batch.clone()],
            |_, batches| batches.push(batch.clone()),
        )
    }
}

impl BatchModifierOpener<PartitionDeltas> for Arc<DeltaCache> {
    fn open_modifier(
        &self,
        partition: &PartitionKey,
        schema: &SchemaRef,
    ) -> Result<BoxFuture<'static, PartitionDeltas>> {
        let batches = self
            .inserts
            .read(partition, |_, batches| batches.clone())
            .unwrap_or_default();
        let schema = schema.clone();
        Ok(Box::pin(async move {
            PartitionDeltas {
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

/// Deltas for a particular partition.
#[derive(Debug)]
pub struct PartitionDeltas {
    schema: SchemaRef,
    inserts: Vec<RecordBatch>,
}

impl BatchModifier for PartitionDeltas {
    fn modify(&self, batch: RecordBatch) -> Result<RecordBatch> {
        Ok(batch) // No modifications to make yet.
    }

    fn stream_rest(&self) -> SendableRecordBatchStream {
        let stream =
            MemoryStream::try_new(self.inserts.clone(), self.schema.clone(), None).unwrap(); // Doesn't error.
        Box::pin(stream)
    }
}
