use crate::errors::Result;
use catalog_types::keys::PartitionId;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::fmt;

/// Strategy for partitioning a table.
pub trait TablePartitionStrategy: Sync + Send + fmt::Debug {
    /// List all partitions this strategy knows about.
    fn list_partitions(&self) -> Result<Vec<PartitionId>>;

    /// Partition a record batch according to this strategy.
    fn partition(&self, batch: RecordBatch) -> Result<BTreeMap<PartitionId, RecordBatch>>;
}

/// A strategy that partitions into a single partition.
#[derive(Debug)]
pub struct SinglePartitionStrategy;

impl TablePartitionStrategy for SinglePartitionStrategy {
    fn list_partitions(&self) -> Result<Vec<PartitionId>> {
        Ok(vec![0])
    }

    fn partition(&self, batch: RecordBatch) -> Result<BTreeMap<PartitionId, RecordBatch>> {
        Ok([(0, batch)].into())
    }
}
