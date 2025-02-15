use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use super::aggregate_hash_table::{AggregateHashTable, AggregateHashTableInsertState};
use super::Aggregates;
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_scan::RowScanState;
use crate::arrays::scalar::ScalarValue;
use crate::execution::operators::hash_aggregate::grouping_value::compute_grouping_value;

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
    Scanning(HashTableScanningOperatorState),
    Uninit,
}

#[derive(Debug)]
struct HashTableBuildingOperatorState {
    /// Remaining inputs we're waiting on to finish.
    remaining: usize,
    /// The global hash table.
    hash_table: AggregateHashTable,
}

#[derive(Debug)]
struct HashTableScanningOperatorState {
    /// Scan states for each partition.
    ///
    /// This will be initialized to the number of partitions, with each
    /// containing a row scan state for indicating which rows that partition
    /// will be scanning.
    scan_states: Vec<HashTableScanPartitionState>,
}

/// Wrapper around a hash table for dealing with a single grouping set.
///
/// Output batch layout: [GROUP_VALS, AGG_RESULTS, GROUPING_VALS]
#[derive(Debug)]
pub struct GroupingSetHashTable {
    layout: AggregateLayout,
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    /// Total number of group expressions.
    num_groups: usize,
    /// Computed group values representing the null bitmask for eaching GROUPING
    /// function.
    grouping_values: Vec<u64>,
    /// Number of partitions writing to and reading from this hash table.
    partitions: usize,
    batch_size: usize,
}

impl GroupingSetHashTable {
    pub fn new(
        aggs: &Aggregates,
        grouping_set: BTreeSet<usize>,
        batch_size: usize,
        partitions: usize,
    ) -> Self {
        let grouping_values: Vec<_> = aggs
            .grouping_functions
            .iter()
            .map(|func| compute_grouping_value(func, &grouping_set))
            .collect();

        // Get group types corresponding to this grouping set, append an
        // additional u64 column for the hash
        let group_types = grouping_set
            .iter()
            .map(|&idx| aggs.groups[idx].datatype())
            .chain([DataType::UInt64]);

        // Total number of groups, not just groups in this grouping set. Used to
        // fill out the null columns.
        let num_groups = aggs.groups.len();

        let layout = AggregateLayout::new(group_types, aggs.aggregates.clone());

        GroupingSetHashTable {
            layout,
            grouping_set,
            num_groups,
            grouping_values,
            partitions,
            batch_size,
        }
    }

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

        state
            .hash_table
            .insert(&mut state.insert_state, &state.groups, &state.inputs)?;

        Ok(())
    }

    /// Merges the local hash table into the operator hash table.
    ///
    /// Returns `true` if this was the last partition we were waiting on,
    /// indicating we can start scanning.
    pub fn merge(
        &self,
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

                if building.remaining == 0 {
                    // We were the last partition to merge, generate all
                    // necessary scan states.
                    let state = std::mem::replace(&mut *op_state, SharedOperatorState::Uninit);
                    let state = match state {
                        SharedOperatorState::Building(state) => state,
                        _ => unreachable!(),
                    };

                    let table = Arc::new(state.hash_table);

                    // Get all types for the groups except for the hash column.
                    let group_types: Vec<_> = {
                        let types = &self.layout.groups.types;
                        types[0..types.len() - 1].to_vec()
                    };

                    let result_types = self
                        .layout
                        .aggregates
                        .iter()
                        .map(|agg| agg.function.return_type.clone());

                    let scan_states = (0..self.partitions)
                        .map(|idx| {
                            let groups = Batch::new(group_types.clone(), self.batch_size)?;
                            let results = Batch::new(result_types.clone(), self.batch_size)?;

                            let mut row_scan_state = RowScanState::new();
                            // Init this row scan state with a subset of blocks
                            // to scan.
                            row_scan_state.reset_for_partial_scan(
                                (0..table.data.num_row_blocks())
                                    .filter(|block_idx| block_idx % idx == 0),
                            );

                            Ok(HashTableScanPartitionState {
                                hash_table: table.clone(),
                                scan_state: RowScanState::new(),
                                groups,
                                results,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    *op_state = SharedOperatorState::Scanning(HashTableScanningOperatorState {
                        scan_states,
                    });

                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Err(RayexecError::new(
                "Operator hash table not in building state",
            )),
        }
    }

    /// Takes a scan state for a partition.
    ///
    /// This should be called once per partition.
    pub fn take_partition_scan_state(
        &self,
        op_state: &HashTableOperatorState,
    ) -> Result<HashTableScanPartitionState> {
        let mut op_state = op_state.inner.lock();
        match &mut *op_state {
            SharedOperatorState::Scanning(scanning) => {
                // Erroring indicates this function was called more than once
                // per partition, or we didn't generate enough scan states to
                // start with.
                let state = scanning
                    .scan_states
                    .pop()
                    .ok_or_else(|| RayexecError::new("Missing partition scan state"))?;

                Ok(state)
            }
            _ => Err(RayexecError::new(
                "Operator hash table not in scanning state",
            )),
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
        //
        // We also scan a subset of the columns to trim off the hash column.
        // That column is accounted for in the layout, but omitted from the
        // grouping set.
        let num_groups_scan = self.grouping_set.len();
        let capacity = state.groups.write_capacity()?;
        state.groups.reset_for_write()?;
        let group_row_count = state.hash_table.data.scan_groups_subset(
            &mut state.scan_state,
            0..num_groups_scan,
            &mut state.groups.arrays,
            capacity,
        )?;

        if group_row_count == 0 {
            // No more groups, we're done.
            output.set_num_rows(0)?;
            return Ok(());
        }

        debug_assert_eq!(capacity, state.results.write_capacity()?);
        debug_assert!(group_row_count <= capacity);

        // Finalize aggregate states.
        unsafe {
            self.layout.finalize_states(
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
        let num_aggs = self.layout.aggregates.len();
        for (src_idx, dest_idx) in (self.num_groups..(self.num_groups + num_aggs)).enumerate() {
            // Swapping should be fine here as well.
            state.groups.swap_arrays(src_idx, (output, dest_idx))?;
        }

        // Append grouping values.
        for (idx, &grouping_val) in self.grouping_values.iter().enumerate() {
            let output_idx = self.num_groups + num_aggs + idx;
            output.set_constant_value(output_idx, ScalarValue::UInt64(grouping_val))?;
        }

        Ok(())
    }
}
