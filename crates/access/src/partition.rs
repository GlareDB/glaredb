use crate::errors::Result;
use crate::exec::{DeltaInsertsExec, LocalPartitionExec, SelectUnorderedExec};
use crate::runtime::AccessRuntime;
use catalog_types::keys::PartitionKey;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use dfutil::cast::cast_record_batch;
use object_store::Error as ObjectStoreError;
use std::fmt;
use std::sync::Arc;
use tracing::{debug, error};

// TODO: A concept of a "remote" partition.

/// A local table partition.
#[derive(Debug, Clone)]
pub struct LocalPartition {
    key: PartitionKey,
    schema: SchemaRef,           // TODO: Shared reference.
    runtime: Arc<AccessRuntime>, // TODO: Shared reference.
}

impl LocalPartition {
    pub fn new(
        key: PartitionKey,
        schema: SchemaRef,
        runtime: Arc<AccessRuntime>,
    ) -> LocalPartition {
        LocalPartition {
            key,
            schema,
            runtime,
        }
    }

    /// Insert a single record batch for this partition.
    pub async fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let batch = cast_record_batch(batch, self.schema.clone())?;
        self.runtime.delta_cache().insert_batch(&self.key, batch);

        if self.should_compact() {
            debug!(key = %self.key, "compacting for partition");
            if let Err(e) = self
                .runtime
                .compactor()
                .compact_deltas(
                    self.runtime.delta_cache().clone(),
                    self.key.clone(),
                    self.schema.clone(),
                )
                .await
            {
                // Shouldn't prevent this insert from returning ok.
                error!(%e, key = %self.key, "failed to compact deltas");
            }
        }

        Ok(())
    }

    fn should_compact(&self) -> bool {
        // TODO: Make this a configurable trigger.
        true
    }

    /// Build an execution plan for scanning this partition.
    pub async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let merr = Into::<DataFusionError>::into; // More readable error mapping.

        let exec: Arc<dyn ExecutionPlan> = match self
            .runtime
            .object_store()
            .head(&self.key.object_path())
            .await
        {
            Ok(meta) => {
                let children: Vec<Arc<dyn ExecutionPlan>> = vec![
                    Arc::new(
                        DeltaInsertsExec::new(
                            self.key.clone(),
                            self.schema.clone(),
                            self.runtime.delta_cache().clone(),
                            projection.clone(),
                        )
                        .map_err(merr)?,
                    ),
                    Arc::new(
                        LocalPartitionExec::new(
                            self.runtime.object_store().clone(),
                            meta,
                            self.schema.clone(),
                            projection.clone(),
                        )
                        .map_err(merr)?,
                    ),
                ];
                Arc::new(SelectUnorderedExec::new(children).map_err(merr)?)
            }
            // This partition isn't backed by a file yet. All data exists
            // entirely in deltas.
            Err(ObjectStoreError::NotFound { .. }) => Arc::new(
                DeltaInsertsExec::new(
                    self.key.clone(),
                    self.schema.clone(),
                    self.runtime.delta_cache().clone(),
                    projection.clone(),
                )
                .map_err(merr)?,
            ),
            Err(e) => return Err(e.into()),
        };

        Ok(exec)
    }
}

impl fmt::Display for LocalPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalPartition(key={})", self.key)
    }
}
