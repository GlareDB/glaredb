use crate::deltacache::{DeltaCache, PartitionDeltaModifier};
use crate::errors::Result;
use crate::keys::PartitionKey;
use crate::modify::{StreamModifier, StreamModifierOpener};
use datafusion::arrow::datatypes::SchemaRef;
use object_store::ObjectStore;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(Debug)]
pub struct Compactor {
    store: Arc<dyn ObjectStore>,
    running_compactions: AtomicU64,
}

impl Compactor {
    /// Compact deltas into the underlying partition file.
    ///
    /// TODO: Locking, race semantics.
    /// TODO: Remove needing schema here.
    pub async fn compact_deltas(
        &self,
        deltas: &DeltaCache,
        partition: &PartitionKey,
        schema: &SchemaRef,
    ) -> Result<()> {
        let _g = CompactionGuard::new(self);

        // let modifier = deltas.open_modifier(partition, schema)?.await;

        // 1. Stream from object store
        // 2. Run through modifier
        // 3. Collect in mem and append remaining
        // 4. Write and put multi part

        unimplemented!()
    }
}

/// A simple guard for tracking compaction runs.
struct CompactionGuard<'a> {
    compactor: &'a Compactor,
}

impl<'a> CompactionGuard<'a> {
    fn new(compactor: &'a Compactor) -> Self {
        let running = compactor
            .running_compactions
            .fetch_add(1, Ordering::Relaxed);
        debug!(running = %running+1, "compaction started");

        CompactionGuard { compactor }
    }
}

impl<'a> Drop for CompactionGuard<'a> {
    fn drop(&mut self) {
        let running = self
            .compactor
            .running_compactions
            .fetch_sub(1, Ordering::Relaxed);
        debug!(running = %running-1, "compaction stopped");
    }
}
