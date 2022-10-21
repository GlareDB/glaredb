use crate::errors::Result;
use crate::exec::{DeltaInsertsExec, LocalPartitionExec, SelectUnorderedExec};
use crate::runtime::AccessRuntime;
use async_trait::async_trait;
use catalog_types::keys::{PartitionId, PartitionKey, TableId};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use dfutil::cast::cast_record_batch;
use object_store::Error as ObjectStoreError;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tracing::{debug, error};

// TODO: Remote partitions.

/// A partition for a table.
///
/// A partition has a one-to-one mapping to a parquet file.
#[derive(Debug)]
pub struct LocalPartition {
    partition: PartitionKey,
    schema: SchemaRef,           // TODO: Use shared reference.
    runtime: Arc<AccessRuntime>, // TODO: Use shared reference.
}

impl LocalPartition {
    pub fn new(
        key: PartitionKey,
        schema: SchemaRef,
        runtime: Arc<AccessRuntime>,
    ) -> LocalPartition {
        LocalPartition {
            partition: key,
            schema,
            runtime,
        }
    }

    /// Insert a batch into this partition.
    ///
    /// There are no checks about the contents of the batches.
    pub async fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let batch = cast_record_batch(batch, self.schema.clone())?;
        self.runtime
            .delta_cache()
            .insert_batch(&self.partition, batch);

        if self.should_compact() {
            debug!(key = %self.partition, "compacting for partition");
            if let Err(e) = self
                .runtime
                .compactor()
                .compact_deltas(
                    self.runtime.delta_cache().clone(),
                    self.partition.clone(),
                    self.schema.clone(),
                )
                .await
            {
                // Shouldn't prevent this insert from returning ok.
                error!(%e, key = %self.partition, "failed to compact deltas");
            }
        }

        Ok(())
    }

    fn should_compact(&self) -> bool {
        // TODO: Make this a configurable trigger.
        true
    }

    pub async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let key = &self.partition;
        let merr = Into::<DataFusionError>::into;

        let exec: Arc<dyn ExecutionPlan> =
            match self.runtime.object_store().head(&key.object_path()).await {
                Ok(meta) => {
                    let children: Vec<Arc<dyn ExecutionPlan>> = vec![
                        Arc::new(
                            DeltaInsertsExec::new(
                                key.clone(),
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
                Err(ObjectStoreError::NotFound { .. }) => Arc::new(
                    DeltaInsertsExec::new(
                        key.clone(),
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
