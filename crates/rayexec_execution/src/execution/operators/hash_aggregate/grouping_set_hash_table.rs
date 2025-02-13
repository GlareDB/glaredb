use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use super::aggregate_hash_table::{AggregateHashTable, AggregateHashTableInsertState};
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::row::row_scan::RowScanState;
use crate::arrays::scalar::ScalarValue;
use crate::logical::logical_aggregate::GroupingFunction;

#[derive(Debug)]
pub struct HashTableBuildPartitionState {
    /// If this table has been merged into the global table.
    finished: bool,
    /// Insert state into the local hash table.
    insert_state: AggregateHashTableInsertState,
    /// The actual hash table.
    hash_table: AggregateHashTable,
    groups: Batch,
    inputs: Batch,
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
    /// Batch for reading the aggregate states from the hash table.
    results: Batch,
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
///
/// Output batch layout: [GROUP_VALS, AGG_RESULTS, GROUPING_VALS]
#[derive(Debug)]
pub struct GroupingSetHashTable {
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    /// Total number of group expressions.
    num_groups: usize,
    /// Computed group values representing the null bitmask for eaching GROUPING
    /// function.
    group_values: Vec<u64>,
}

impl GroupingSetHashTable {
    pub fn insert(
        &mut self,
        state: &mut HashTableBuildPartitionState,
        input: &mut Batch,
    ) -> Result<()> {
        if state.finished {
            return Err(RayexecError::new("Partition-local table already finished"));
        }

        // Get both the group arrays, and the inputs to the aggregates.
        for (dest_idx, &src_idx) in self.grouping_set.iter().enumerate() {
            state.groups.swap_arrays(dest_idx, (input, src_idx))?;
        }

        for (dest_idx, src_idx) in state
            .hash_table
            .layout
            .aggregates
            .iter()
            .flat_map(|agg| agg.columns.iter().map(|col| col.idx))
            .enumerate()
        {
            // Can't swap, multiple aggregates may be referencing the same
            // column.
            state.inputs.clone_array_from(dest_idx, (input, src_idx))?;
        }

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
    /// The output batch should have groups first, then aggregate columns, then
    /// finally any GROUPING values.
    ///
    /// The output batch must be writeable.
    pub fn scan(&self, state: &mut HashTableScanPartitionState, output: &mut Batch) -> Result<()> {
        // Scan just the groups from this hash table. This fills the pointers to
        // use for scanning and finalizing the aggregate states.
        let capacity = state.groups.write_capacity()?;
        state.groups.reset_for_write()?;
        let group_count = state.hash_table.data.scan_groups(
            &mut state.scan_state,
            &mut state.groups.arrays,
            capacity,
        )?;

        if group_count == 0 {
            // No more groups, we're done.
            output.set_num_rows(0)?;
            return Ok(());
        }

        debug_assert_eq!(capacity, state.results.write_capacity()?);
        debug_assert!(group_count <= capacity);

        // Finalize aggregate states.
        unsafe {
            state.hash_table.layout.finalize_states(
                state.scan_state.scanned_row_pointers_mut(),
                &mut state.results.arrays,
            )?;
        }

        // Write groups to the output. This will write in the order of the
        // groups, but have them densely packed on the left.
        //
        // Groups not in the grouping set will have the output array modified to
        // be all nulls.
        let mut group_idx = 0;
        for output_idx in 0..self.num_groups {
            if self.grouping_set.contains(&output_idx) {
                // Swapping arrays is fine since each group is only written once to
                // the output.
                state.groups.swap_arrays(group_idx, (output, output_idx))?;
                group_idx += 1;
            } else {
                let arr = &mut output.arrays[output_idx];
                arr.put_validity(Validity::new_all_invalid(arr.logical_len()))?;
            }
        }

        // Write out the aggregate results.
        let num_aggs = state.hash_table.layout.aggregates.len();
        for (src_idx, dest_idx) in (self.num_groups..(self.num_groups + num_aggs)).enumerate() {
            // Swapping should be fine here as well.
            state.groups.swap_arrays(src_idx, (output, dest_idx))?;
        }

        // Append grouping values.
        for (idx, &grouping_val) in self.group_values.iter().enumerate() {
            let output_idx = self.num_groups + num_aggs + idx;
            output.set_constant_value(output_idx, ScalarValue::UInt64(grouping_val))?;
        }

        Ok(())
    }
}

fn compute_grouping_value(grouping: &GroupingFunction, grouping_set: &BTreeSet<usize>) -> u64 {
    let mut grouping_val: u64 = 0;

    // Reverse iter to match postgres, the right-most
    // column the GROUPING corresponds to the least
    // significant bit.
    for (idx, group_col) in grouping.group_exprs.iter().rev().enumerate() {
        if !grouping_set.contains(group_col) {
            // "null mask"
            grouping_val |= 1 << idx;
        }
    }

    grouping_val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_values_no_mask() {
        let grouping_set: BTreeSet<usize> = [0, 1].into_iter().collect();

        // GROUPING(col0, col1) FROM t GROUP BY col0, col1
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col1) FROM t GROUP BY col0, col1
        let grouping = GroupingFunction {
            group_exprs: vec![1],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0) FROM t GROUP BY col0, col1
        let grouping = GroupingFunction {
            group_exprs: vec![0],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));
    }

    #[test]
    fn grouping_values_mask() {
        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (col0, col1)
        let grouping_set: BTreeSet<usize> = [0, 1].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(0, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (col0, NULL)
        let grouping_set: BTreeSet<usize> = [0].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(1, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (NULL, col1)
        let grouping_set: BTreeSet<usize> = [1].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(2, compute_grouping_value(&grouping, &grouping_set));

        // GROUPING(col0, col1) FROM t GROUP BY CUBE (col0, col1)
        // GROUPING SET (NULL, NULL)
        let grouping_set: BTreeSet<usize> = [].into_iter().collect();
        let grouping = GroupingFunction {
            group_exprs: vec![0, 1],
        };
        assert_eq!(3, compute_grouping_value(&grouping, &grouping_set));
    }
}
