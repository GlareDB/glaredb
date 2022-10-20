use crate::errors::Result;
use crate::exec::{DeltaInsertsExec, LocalPartitionExec, SelectUnorderedExec};
use crate::partition::LocalPartition;
use crate::runtime::AccessRuntime;
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
use futures::try_join;
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

pub trait PartitionStrategy: Sync + Send + fmt::Debug {
    /// List all partitions this strategy knows about.
    fn list_partitions(&self) -> Result<Vec<PartitionId>>;

    /// Partition a record batch according this strategy.
    fn partition(&self, batch: RecordBatch) -> Result<BTreeMap<PartitionId, RecordBatch>>;
}

/// A table backed by one or more partitions.
pub struct Table {
    table: TableKey,
    strat: Box<dyn PartitionStrategy>,
    runtime: Arc<AccessRuntime>,
    schema: SchemaRef,
}

impl Table {
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
impl TableProvider for Table {
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
        unimplemented!()
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Table(table_id={})", self.table)
    }
}
