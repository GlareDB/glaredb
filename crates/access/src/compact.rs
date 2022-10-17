use crate::deltacache::{DeltaCache, PartitionDeltaModifier};
use crate::errors::Result;
use crate::keys::PartitionKey;
use crate::modify::{StreamModifier, StreamModifierOpener};
use crate::parquet::{ParquetOpener, ParquetUploader};
use crate::partitionexec::PartitionStreamOpener;
use datafusion::arrow::compute::kernels::concat::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::EmptyRecordBatchStream;
use futures::StreamExt;
use object_store::{Error as ObjectStoreError, ObjectStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, trace};

const UPLOAD_BUF_CAPACITY: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub struct Compactor {
    store: Arc<dyn ObjectStore>,
    running_compactions: AtomicU64,
}

impl Compactor {
    pub fn new(store: Arc<dyn ObjectStore>) -> Compactor {
        Compactor {
            store,
            running_compactions: 0.into(),
        }
    }

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

        // 1. Stream from object store
        // 2. Run through modifier
        // 3. Collect in mem and append remaining
        // 4. Write and put multi part

        let path = partition.object_path();
        // TODO: Do we want to cache metas to avoid needing to fetch it
        // everytime?
        let mut stream = match self.store.head(&path).await {
            Ok(meta) => {
                let opener = ParquetOpener {
                    store: self.store.clone(),
                    meta: meta.clone(),
                    meta_size_hint: None,
                    projection: None,
                };
                opener.open()?.await?
            }
            Err(ObjectStoreError::NotFound { .. }) => {
                debug!(%partition, "no file for partition, using empty stream");
                EmptyRecordBatchStream::new(schema.clone()).boxed()
            }
            Err(e) => return Err(e.into()),
        };

        let modifier = deltas.open_modifier(partition, schema)?.await;

        // TODO: We'll want to have a "mutable" record batch eventually and just
        // append directly to that.

        // Modify existing batches.
        let (lower, _) = stream.size_hint();
        let mut batches = Vec::with_capacity(lower);
        while let Some(result) = stream.next().await {
            let batch = result?;
            let modified = modifier.modify(batch)?;
            batches.push(modified);
        }

        // Get remaining batches.
        let mut stream = modifier.stream_rest();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

        let concat = concat_batches(schema, &batches)?;

        // TODO: Sort, rechunk.

        let uploader = ParquetUploader {
            store: self.store.clone(),
            path,
        };
        // TODO: Reuse buffers.
        let mut buf = Vec::with_capacity(UPLOAD_BUF_CAPACITY);
        uploader
            .upload_batches(schema.clone(), [concat], &mut buf)
            .await?;

        deltas.remove_partition_deltas(partition);

        Ok(())
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
