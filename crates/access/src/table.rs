use crate::errors::Result;
use crate::exec::{DeltaInsertsExec, LocalPartitionExec, SelectUnorderedExec};
use crate::partition::LocalPartition;
use crate::runtime::AccessRuntime;
use crate::strategy::PartitionStrategy;
use async_trait::async_trait;
use catalog_types::keys::{PartitionId, PartitionKey, TableId, TableKey};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// A table backed by one or more partitions.
// TODO: Use shared references.
pub struct PartitionedTable {
    table: TableKey,
    strat: Box<dyn PartitionStrategy>,
    runtime: Arc<AccessRuntime>,
    schema: SchemaRef,
}

impl PartitionedTable {
    pub fn new(
        key: TableKey,
        strat: Box<dyn PartitionStrategy>,
        runtime: Arc<AccessRuntime>,
        schema: SchemaRef,
    ) -> PartitionedTable {
        PartitionedTable {
            table: key,
            strat,
            runtime,
            schema,
        }
    }

    /// Insert a batch, partitioning as necessary.
    pub async fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let batches = self.strat.partition(batch)?;

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
        let partitions = self.strat.list_partitions().map_err(|e| {
            DataFusionError::Plan(format!("build plan for table partitions: {}", e))
        })?;
        let mut plans = Vec::with_capacity(partitions.len());

        for part_id in partitions {
            // TODO: Same as above, need to determine if this is local/remote,
            // and do all of these in parallel.
            let partition = LocalPartition::new(
                self.table.partition_key(part_id),
                self.schema.clone(),
                self.runtime.clone(),
            );
            let plan = partition.scan(ctx, projection, filters, limit).await?;
            plans.push(plan);
        }

        let plan = SelectUnorderedExec::new(plans)
            .map_err(|e| DataFusionError::Plan(format!("build select unordered plan: {}", e)))?;

        Ok(Arc::new(plan))
    }
}

impl fmt::Display for PartitionedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table(table_id={})", self.table)
    }
}
