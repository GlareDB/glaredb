use crate::deltacache::DeltaCache;
use crate::errors::Result;
use crate::exec::{DeltaInsertsExec, LocalPartitionExec, SelectUnorderedExec};
use crate::keys::PartitionKey;
use crate::parquet::ParquetUploader;
use datafusion::arrow::compute::kernels::concat::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use object_store::{Error as ObjectStoreError, ObjectStore};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, trace};

const UPLOAD_BUF_CAPACITY: usize = 2 * 1024 * 1024;

pub struct Compactor {
    store: Arc<dyn ObjectStore>,
    running_compactions: AtomicU64,
    /// Dummy context to be able to run datafusion execution plans during
    /// compaction.
    dummy_context: Arc<TaskContext>,
}

impl Compactor {
    pub fn new(store: Arc<dyn ObjectStore>) -> Compactor {
        // NOTE: None of these value currently matter.
        let dummy_context = Arc::new(TaskContext::new(
            "compact".to_string(),
            "compact".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        ));

        Compactor {
            store,
            running_compactions: 0.into(),
            dummy_context,
        }
    }

    /// Compact deltas into the underlying partition file.
    ///
    /// TODO: Locking, race semantics.
    /// TODO: Remove needing schema here.
    pub async fn compact_deltas(
        &self,
        deltas: Arc<DeltaCache>,
        partition: PartitionKey,
        input_schema: SchemaRef,
    ) -> Result<()> {
        let _g = CompactionGuard::new(self);

        let path = partition.object_path();
        // TODO: Do we want to cache metas to avoid needing to fetch it
        // everytime?
        let exec: Arc<dyn ExecutionPlan> = match self.store.head(&path).await {
            Ok(meta) => {
                trace!(%partition, "compacting deltas with existing partition file");
                let children: Vec<Arc<dyn ExecutionPlan>> = vec![
                    Arc::new(DeltaInsertsExec::new(
                        partition.clone(),
                        input_schema.clone(),
                        deltas.clone(),
                        None,
                    )?),
                    Arc::new(LocalPartitionExec::new(
                        self.store.clone(),
                        meta,
                        input_schema.clone(),
                        None,
                    )?),
                ];
                Arc::new(SelectUnorderedExec::new(children)?)
            }
            Err(ObjectStoreError::NotFound { .. }) => {
                trace!(%partition, "compacting deltas into new file");
                Arc::new(DeltaInsertsExec::new(
                    partition.clone(),
                    input_schema.clone(),
                    deltas.clone(),
                    None,
                )?)
            }
            Err(e) => return Err(e.into()),
        };

        // TODO: We'll want to have a "mutable" record batch eventually and just
        // append directly to that.

        let stream = exec.execute(0, self.dummy_context.clone())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let concat = concat_batches(&input_schema, &batches)?;
        debug!(num_rows = %concat.num_rows(), %partition, "concatenated batch for compaction");

        // TODO: Sort, rechunk.

        let uploader = ParquetUploader {
            store: self.store.clone(),
            path,
        };
        // TODO: Reuse buffers.
        let mut buf = Vec::with_capacity(UPLOAD_BUF_CAPACITY);
        uploader
            .upload_batches(input_schema.clone(), [concat], &mut buf)
            .await?;

        deltas.remove_partition_deltas(&partition);

        Ok(())
    }
}

impl fmt::Debug for Compactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Compactor({:?}, running={})",
            self.store,
            self.running_compactions.load(Ordering::Relaxed)
        )
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
