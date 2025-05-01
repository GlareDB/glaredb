use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use super::base::{BaseHashTable, BaseHashTableInsertState};
use crate::arrays::batch::Batch;

/// Compute the partition to use for a given hash value.
pub const fn partition(hash: u64, partitions: usize) -> usize {
    ((hash as u128 * partitions as u128) >> 64) as usize
}

#[derive(Debug)]
pub struct PartitionedHashTablePartitionState {
    /// Reusable hashes buffer.
    hashes: Vec<u64>,
    /// Reusable partition selector state.
    partition_selector: PartitionSelector,
    /// Hash tables partitioned by hash values.
    tables: Vec<BaseHashTable>,
    /// Insert for the hash tables.
    states: Vec<BaseHashTableInsertState>,
}

#[derive(Debug)]
pub struct PartitionedHashTableOperatorState {
    /// Tables that have been flushed per partition.
    flushed: Vec<Mutex<FlushedTables>>,
}

#[derive(Debug)]
struct FlushedTables {
    /// Tables that have been flush so far.
    tables: Vec<BaseHashTable>,
}

/// Hash table that partitions based on a row's hash.
#[derive(Debug)]
pub struct PartitionedHashTable {}

impl PartitionedHashTable {
    /// Inserts a batch into this partition's local hash tables.
    ///
    /// Internally partitions the input.
    pub fn insert_local(
        &self,
        state: &mut PartitionedHashTablePartitionState,
        agg_selection: &[usize],
        groups: &Batch,
        inputs: &Batch,
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

#[derive(Debug)]
struct PartitionSelector {
    /// Partition to use for each row in the input.
    partition_indices: Vec<usize>,
    /// Total counts for each partition.
    counts: Vec<usize>,
}

impl PartitionSelector {
    fn new(partition_count: usize) -> Self {
        PartitionSelector {
            partition_indices: Vec::new(),
            counts: vec![0; partition_count],
        }
    }

    /// Resets the selector state by recomputing partitoin indices using the
    /// provided hashes.
    fn reset_using_hashes(&mut self, hashes: &[u64]) {
        let partition_count = self.counts.len();
        self.partition_indices.clear();
        self.partition_indices
            .extend(hashes.iter().map(|&hash| partition(hash, partition_count)));

        // Compute partition totals.
        self.counts.as_mut_slice().fill(0);
        for &idx in &self.partition_indices {
            self.counts[idx] += 1;
        }
    }

    fn row_indices_for_partition(&self, partition: usize) -> RowIndexIter {
        RowIndexIter {
            partition,
            rem_count: self.counts[partition],
            curr: 0,
            partition_indices: &self.partition_indices,
        }
    }
}

/// Iterator for emitting row indices for a given partition.
#[derive(Debug)]
struct RowIndexIter<'a> {
    partition: usize,
    rem_count: usize,
    curr: usize,
    partition_indices: &'a [usize],
}

impl Iterator for RowIndexIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.rem_count == 0 {
                return None;
            }

            let row_idx = self.curr;
            self.curr += 1;
            if self.partition_indices[row_idx] == self.partition {
                self.rem_count -= 1;
                return Some(row_idx);
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem_count, Some(self.rem_count))
    }
}

impl ExactSizeIterator for RowIndexIter<'_> {}
