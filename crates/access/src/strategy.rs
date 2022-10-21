use crate::errors::Result;
use catalog_types::keys::PartitionId;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::fmt;

pub trait PartitionStrategy: Sync + Send + fmt::Debug {
    /// List all partitions this strategy knows about.
    fn list_partitions(&self) -> Result<Vec<PartitionId>>;

    /// Partition a record batch according this strategy.
    fn partition(&self, batch: RecordBatch) -> Result<BTreeMap<PartitionId, RecordBatch>>;
}

/// A strategy with a single partition. Should only be used for
/// testing/development.
#[derive(Debug)]
pub struct SinglePartitionStrategy;

impl PartitionStrategy for SinglePartitionStrategy {
    fn list_partitions(&self) -> Result<Vec<PartitionId>> {
        Ok(vec![0])
    }

    fn partition(&self, batch: RecordBatch) -> Result<BTreeMap<PartitionId, RecordBatch>> {
        Ok([(0, batch)].into())
    }
}
