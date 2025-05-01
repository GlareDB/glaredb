use glaredb_error::{DbError, Result};

use crate::arrays::batch::Batch;

/// Compute the partition to use for a given hash value.
pub const fn partition(hash: u64, partitions: usize) -> usize {
    ((hash as u128 * partitions as u128) >> 64) as usize
}

#[derive(Debug)]
pub struct PartitionedHashTablePartitionState {}

#[derive(Debug)]
pub struct PartitionedHashTableOperatorState {}

/// Hash table that partitions based on a row's hash.
#[derive(Debug)]
pub struct PartitionedHashTable {}

impl PartitionedHashTable {
    /// Inserts a batch into this partition's local hash tables.
    ///
    /// Internally partitions the input.
    pub fn insert_local(
        &self,
        state: &mut PartitionedHashTableOperatorState,
        input: &mut Batch,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Flushes the local tables to the operator state.
    ///
    /// Should only be called once per partition.
    pub fn flush(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Merges the global hash tables.
    pub fn merge_global(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &PartitionedHashTablePartitionState,
    ) -> Result<()> {
        unimplemented!()
    }
}
