use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use super::aggregate_hash_table::{AggregateHashTable, AggregateHashTableInsertState};
use crate::arrays::batch::Batch;
use crate::arrays::row::row_scan::RowScanState;

#[derive(Debug)]
pub struct HashTableBuildPartitionState {
    /// If this table has been merged into the global table.
    finished: bool,
    /// Insert state into the local hash table.
    insert_state: AggregateHashTableInsertState,
    /// The actual hash table.
    hash_table: AggregateHashTable,
}

#[derive(Debug)]
pub struct HashTableScanPartitionState {
    /// The final hash table.
    hash_table: Arc<AggregateHashTable>,
    /// Scan state for scanning the hash table. Initialized to scan only the
    /// blocks for this partition.
    scan_state: RowScanState,
    /// Batch for reading groups from the hash table.
    groups: Batch,
}

#[derive(Debug)]
pub struct HashTableOperatorState {
    inner: Mutex<SharedOperatorState>,
}

#[derive(Debug)]
enum SharedOperatorState {
    Building(HashTableBuildingOperatorState),
    Uninit,
}

#[derive(Debug)]
struct HashTableBuildingOperatorState {
    /// Remaining inputs we're waiting on to finish.
    remaining: usize,
    /// The global hash table.
    hash_table: AggregateHashTable,
}

/// Wrapper around a hash table for dealing with a single grouping set.
#[derive(Debug)]
pub struct GroupingSetHashTable {
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    /// Group indices that are not in the grouping set.
    null_indices: Vec<usize>,
}

impl GroupingSetHashTable {
    pub fn insert(
        &mut self,
        state: &mut HashTableBuildPartitionState,
        input: &Batch,
    ) -> Result<()> {
        if state.finished {
            return Err(RayexecError::new("Partition-local table already finished"));
        }

        // Get both the group arrays, and the inputs to the aggregates.
        //
        // TODO: Try not allocate here.
        let groups: Vec<_> = self
            .grouping_set
            .iter()
            .map(|&col_idx| &input.arrays[col_idx])
            .collect();
        let agg_inputs: Vec<_> = state
            .hash_table
            .layout
            .aggregates
            .iter()
            .flat_map(|agg| agg.columns.iter().map(|col| &input.arrays[col.idx]))
            .collect();

        unimplemented!()
        // state.hash_table.insert(
        //     &mut state.insert_state,
        //     &groups,
        //     &agg_inputs,
        //     input.num_rows(),
        // )?;

        // Ok(())
    }

    /// Merges the local hash table into the operator hash table.
    ///
    /// Returns `true` if this was the last partition we were waiting on,
    /// indicating we can start scanning.
    pub fn merge(
        &mut self,
        op_state: &HashTableOperatorState,
        state: &mut HashTableBuildPartitionState,
    ) -> Result<bool> {
        if state.finished {
            return Err(RayexecError::new("State already finished"));
        }

        let mut op_state = op_state.inner.lock();

        match &mut *op_state {
            SharedOperatorState::Building(building) => {
                building
                    .hash_table
                    .merge_from(&mut state.insert_state, &mut state.hash_table)?;

                building.remaining -= 1;

                // TODO: Flip state

                Ok(building.remaining == 0)
            }
            _ => Err(RayexecError::new("Operator hash table in invalid state")),
        }
    }

    /// Scan the hashtable writing to `output`.
    ///
    /// This can be called in parallel with other partitions as each state
    /// contains a disjoint set of blocks to scan.
    ///
    /// The output batch should have groups first, then aggregate columns.
    ///
    /// The output batch must be writeable.
    pub fn scan(&self, state: &mut HashTableScanPartitionState, output: &mut Batch) -> Result<()> {
        // Scan just the groups from this grouping set.
        // let groups = self.grouping_set.iter().map(|&idx| &mut output.arrays[idx]);

        unimplemented!()
    }
}
