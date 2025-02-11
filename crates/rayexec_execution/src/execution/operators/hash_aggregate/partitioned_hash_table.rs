use std::collections::BTreeSet;

use rayexec_error::Result;

use super::aggregate_hash_table::AggregateHashTable;
use crate::arrays::batch::Batch;

#[derive(Debug)]
pub struct BuildPartitionState {
    /// Partition-local hash table.
    hash_table: AggregateHashTable,
}

#[derive(Debug)]
pub struct PartitionedAggregateHashTable {
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    /// Indices of the columns which are not part of this grouping set.
    null_mask: Vec<usize>,
}

impl PartitionedAggregateHashTable {
    /// Sinks an input batch for a partition.
    ///
    /// This will write the batch and computed aggregates to the partition-local
    /// hash table.
    pub fn sink_partition(&self, state: &mut BuildPartitionState, input: &Batch) -> Result<()> {
        let groups: Vec<_> = self
            .grouping_set
            .iter()
            .map(|&idx| &input.arrays[idx])
            .collect();

        unimplemented!()
    }
}
