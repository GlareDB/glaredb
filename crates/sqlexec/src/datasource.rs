use crate::errors::Result;
use crate::runtime::AccessRuntime;
use access::errors::AccessError;
use access::exec::{
    DeltaInsertsExec, LocalPartitionExec, SelectUnorderedExec, SingleOpaqueTraceExec,
};
use access::keys::{PartitionKey, TableId};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{empty::EmptyExec, ExecutionPlan};
use dfutil::cast::cast_record_batch;
use object_store::Error as ObjectStoreError;
use std::any::Any;
use std::sync::Arc;
use tracing::{debug, error, trace};

/// An implementation of a table provider using our delta cache.
///
/// NOTE: This currently has a one-to-one mapping between table and partition.
/// There will be an additional layer between `DeltaMergeExec` and this to
/// combine access to multiple partitions for a table.
#[derive(Debug, Clone)]
pub struct DeltaTable {
    table_id: TableId,
    schema: SchemaRef,
    runtime: Arc<AccessRuntime>,
}

impl DeltaTable {
    pub fn new(table_id: TableId, schema: SchemaRef, runtime: Arc<AccessRuntime>) -> DeltaTable {
        DeltaTable {
            table_id,
            schema,
            runtime,
        }
    }

    pub async fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        // NOTE: All of this functionality will be moved to some other 'table'
        // abtractions which will handle cross-partition inserts.

        let key = PartitionKey {
            table_id: self.table_id,
            part_id: 0, // TODO: Need another layer of indirection.
        };
        let batch = cast_record_batch(batch, self.schema.clone())?;
        self.runtime.delta_cache().insert_batch(&key, batch);

        // TODO: Make this a configurable trigger.
        if self.should_compact() {
            debug!(%key, "compacting for partition");
            if let Err(e) = self
                .runtime
                .compactor()
                .compact_deltas(
                    self.runtime.delta_cache().clone(),
                    key.clone(),
                    self.schema.clone(),
                )
                .await
            {
                // Shouldn't prevent this insert from returning ok.
                error!(%e, %key, "failed to compact deltas");
            }
        }

        Ok(())
    }

    fn should_compact(&self) -> bool {
        true
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let key = PartitionKey {
            table_id: self.table_id,
            part_id: 0,
        };

        // TODO: Project schema.

        let merr = |e: AccessError| Into::<DataFusionError>::into(e);

        let exec: Arc<dyn ExecutionPlan> =
            match self.runtime.object_store().head(&key.object_path()).await {
                Ok(meta) => Arc::new(
                    SelectUnorderedExec::new(
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
                    )
                    .map_err(merr)?,
                ),
                Err(ObjectStoreError::NotFound { .. }) => {
                    Arc::new(EmptyExec::new(false, self.schema.clone()))
                }
                Err(e) => return Err(e.into()),
            };

        let exec = Arc::new(SingleOpaqueTraceExec::new(exec));

        Ok(exec)
    }
}
