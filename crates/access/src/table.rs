use crate::errors::{AccessError, Result};
use crate::exec::SelectUnorderedExec;
use crate::partition::LocalPartition;
use crate::runtime::AccessRuntime;
use crate::strategy::TablePartitionStrategy;
use async_trait::async_trait;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::MutableTableProvider;
use catalog_types::keys::TableKey;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tracing::trace;

/// A table backed by one or more partitions.
// TODO: Use shared references.
#[derive(Debug)]
pub struct PartitionedTable {
    table: TableKey,
    strategy: Box<dyn TablePartitionStrategy>,
    runtime: Arc<AccessRuntime>,
    schema: SchemaRef,
}

impl PartitionedTable {
    pub fn new(
        key: TableKey,
        strategy: Box<dyn TablePartitionStrategy>,
        runtime: Arc<AccessRuntime>,
        schema: SchemaRef,
    ) -> PartitionedTable {
        PartitionedTable {
            table: key,
            strategy,
            runtime,
            schema,
        }
    }

    pub async fn scan_inner(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self.strategy.list_partitions()?;
        trace!(?partitions, "scanning partitioned table");
        let mut plans = Vec::with_capacity(partitions.len());

        for part_id in partitions {
            // TODO: Same as above, need to determine if this is local/remote,
            // and do all of these in parallel.
            let partition = LocalPartition::new(
                self.table.partition_key(part_id),
                self.schema.clone(),
                self.runtime.clone(),
            );
            trace!(?partition, "including partition in scan");
            let plan = partition.scan(ctx, projection, filters, limit).await?;
            plans.push(plan);
        }

        let plan = SelectUnorderedExec::new(plans)?;

        Ok(Arc::new(plan))
    }
}

#[async_trait]
impl MutableTableProvider for PartitionedTable {
    type Error = AccessError;

    async fn insert(&self, _ctx: &SessionContext, batch: RecordBatch) -> Result<()> {
        let batches = self.strategy.partition(batch)?;

        for (part_id, batch) in batches {
            let key = self.table.partition_key(part_id);

            // TODO: We'll need some metadata to determine if the partition is
            // local or remote.
            let partition = LocalPartition::new(key, self.schema.clone(), self.runtime.clone());

            // TODO: Run all inserts in parallel.
            //
            // TODO: If we compact and split, we should be updating the
            // strategy.
            partition.insert_batch(batch).await?;
        }

        Ok(())
    }
}

// The scan on `Table` represents a base table scan. Index scans will happen on
// some other struct.
#[async_trait]
impl TableProvider for PartitionedTable {
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
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let plan = self
            .scan_inner(ctx, projection, filters, limit)
            .await
            .map_err(|e| DataFusionError::Plan(format!("build plan for table scan: {}", e)))?;
        Ok(plan)
    }
}

impl fmt::Display for PartitionedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table(table_id={})", self.table)
    }
}
