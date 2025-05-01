use std::collections::BTreeSet;
use std::sync::Arc;

use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use super::base::{BaseHashTable, BaseHashTableInsertState};
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalU64};
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::compute::hash::hash_many_arrays;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_scan::RowScanState;
use crate::arrays::scalar::ScalarValue;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::execution::operators::hash_aggregate::hash_table::base::NO_GROUPS_HASH_VALUE;
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::util::iter::IntoExactSizeIterator;

/// Compute the partition to use for a given hash value.
pub const fn partition(hash: u64, partitions: usize) -> usize {
    ((hash as u128 * partitions as u128) >> 64) as usize
}

#[derive(Debug)]
pub enum PartitionedHashTablePartitionState {
    /// Partition is building its local hash tables.
    Building(LocalBuildingState),
    /// Partition has flushed its local hash tables to the operator state.
    MergeReady { partition_idx: usize },
    /// Partition has merged its tables.
    ScanReady { partition_idx: usize },
    /// Partition is scanning.
    Scanning(LocalScanningState),
}

#[derive(Debug)]
pub struct LocalBuildingState {
    partition_idx: usize,
    /// Reusable partition selector state.
    partition_selector: PartitionSelector,
    /// Hash tables partitioned by hash values.
    tables: Vec<BaseHashTable>,
    /// Insert for the hash tables.
    states: Vec<BaseHashTableInsertState>,
}

#[derive(Debug)]
pub struct LocalScanningState {
    /// References to the final hash tables.
    tables: Vec<Arc<BaseHashTable>>,
    /// Scan states for each of the tables.
    scan_states: Vec<RowScanState>,
    /// Batch for reading groups from the hash table.
    groups: Batch,
    /// Batch for reading the aggregate states from the hash table.
    results: Batch,
}

#[derive(Debug)]
pub struct PartitionedHashTableOperatorState {
    /// Tables that have been flushed per partition.
    ///
    /// When partitions are merging, the flushed tables get drained into a final
    /// global table.
    flushed: Vec<Mutex<FlushedTables>>,
    /// The final aggregate tables.
    ///
    /// None until a partition builds the final table for a given partition
    /// index.
    final_tables: Vec<Mutex<Option<Arc<BaseHashTable>>>>,
    /// Output types of the tables.
    ///
    /// Here for convenience.
    result_types: Vec<DataType>,
    /// Batch size to use when scanning.
    batch_size: usize,
}

#[derive(Debug)]
struct FlushedTables {
    /// Tables that have been flush so far.
    tables: Vec<BaseHashTable>,
}

/// Hash table that partitions based on a row's hash.
#[derive(Debug)]
pub struct PartitionedHashTable {
    layout: AggregateLayout,
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    groups: Vec<PhysicalColumnExpr>,
    /// Computed group values representing the null bitmask for eaching GROUPING
    /// function.
    grouping_values: Vec<i64>,
}

impl PartitionedHashTable {
    /// Returns an iterator of all group types except for the hash column.
    ///
    /// Used when creating batches for buffering user-provided group columns.
    fn group_types_no_hash_iter(&self) -> impl IntoExactSizeIterator<Item = DataType> + '_ {
        let types = &self.layout.groups.types;
        types.iter().take(types.len() - 1).cloned()
    }

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
        debug_assert_eq!(groups.num_rows, inputs.num_rows);

        let state = match state {
            PartitionedHashTablePartitionState::Building(building) => building,
            _ => {
                return Err(DbError::new(
                    "Invalid partition state, cannot insert into local tables",
                ));
            }
        };

        // Hash the groups.
        // TODO: Avoid allocating here.
        let mut hashes_arr = Array::new(&DefaultBufferManager, DataType::UInt64, groups.num_rows)?;
        let hashes = PhysicalU64::get_addressable_mut(&mut hashes_arr.data)?.slice;
        hash_many_arrays(&groups.arrays, 0..groups.num_rows, hashes)?;

        if groups.arrays.is_empty() {
            // We didn't actually hash anything...
            //
            // But we still want to insert these inputs. Since we treat the hash
            // values as part of the group, we just fill the hash slice with the
            // same value to produce a single group. The value we use is
            // non-zero to aid in debuggability.
            //
            // No group arrays will happen when dealing with rollups, since the
            // final rollup will be with no groups.
            hashes.fill(NO_GROUPS_HASH_VALUE);
        }

        // Compute partitions for each row.
        let selector = &mut state.partition_selector;
        selector.reset_using_hashes(&hashes);

        debug_assert_eq!(state.tables.len(), state.states.len());

        // Now insert into each partition table.
        for (partition_idx, (table, table_state)) in
            state.tables.iter_mut().zip(&mut state.states).enumerate()
        {
            let row_sel = state
                .partition_selector
                .row_indices_for_partition(partition_idx);

            table.insert_with_hashes(
                table_state,
                agg_selection,
                row_sel,
                groups,
                inputs,
                &hashes_arr,
            )?;
        }

        Ok(())
    }

    /// Flushes the local tables to the operator state.
    ///
    /// Should only be called once per partition.
    pub fn flush(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
    ) -> Result<()> {
        let building = match state {
            PartitionedHashTablePartitionState::Building(building) => building,
            _ => {
                return Err(DbError::new(
                    "Partition in invalid state, cannot flush tables",
                ));
            }
        };

        debug_assert_eq!(building.tables.len(), op_state.flushed.len());

        for (partition_idx, table) in building.tables.drain(..).enumerate() {
            let mut flushed = op_state.flushed[partition_idx].lock();
            flushed.tables.push(table);
        }

        *state = PartitionedHashTablePartitionState::MergeReady {
            partition_idx: building.partition_idx,
        };

        Ok(())
    }

    /// Merges the global hash tables.
    pub fn merge_global(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
    ) -> Result<()> {
        let partition_idx = match state {
            PartitionedHashTablePartitionState::MergeReady { partition_idx } => *partition_idx,
            _ => {
                return Err(DbError::new(
                    "Partition in invalid state, cannot merge tables",
                ));
            }
        };

        // Merge only the tables for this partition index.
        //
        // All partitions should be merge an independ set of tables.
        let mut flushed = op_state.flushed[partition_idx].lock();
        if flushed.tables.len() != op_state.flushed.len() {
            // Means not all partitions flushed.
            return Err(DbError::new(
                "Attempted to merge into final table, but some tables missing",
            ));
        }

        // Pick arbitrary table to be the global table we merge into.
        let mut global = flushed.tables.pop().expect("at least one table");
        let mut insert_state = global.init_insert_state();

        for mut table in flushed.tables.drain(..) {
            global.merge_from(
                &mut insert_state,
                0..self.layout.aggregates.len(),
                &mut table,
            )?;
        }

        // Now put it in the global state.
        let mut final_table = op_state.final_tables[partition_idx].lock();
        debug_assert!(final_table.is_none());
        *final_table = Some(Arc::new(global));

        *state = PartitionedHashTablePartitionState::ScanReady { partition_idx };

        Ok(())
    }

    pub fn scan(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
        output: &mut Batch,
    ) -> Result<()> {
        let state = match state {
            PartitionedHashTablePartitionState::ScanReady { .. } => {
                self.prepare_scan(op_state, state)?;
                match state {
                    PartitionedHashTablePartitionState::Scanning(scanning) => scanning,
                    _ => panic!("Expected prepare to update state"),
                }
            }
            PartitionedHashTablePartitionState::Scanning(scanning) => scanning,
            _ => return Err(DbError::new("Partition in invalid state, cannot scan")),
        };

        // We have multiple tables to scan. If a scan returns 0 rows, move to
        // the next table
        loop {
            let (table, scan_state) = match state.tables.last() {
                Some(table) => (
                    table,
                    state
                        .scan_states
                        .last_mut()
                        .expect("tables and states to be the same length"),
                ),
                None => {
                    // No more tables.
                    output.set_num_rows(0)?;
                    return Ok(());
                }
            };

            // Scan just the groups from this hash table. This fills the pointers to
            // use for scanning and finalizing the aggregate states.
            //
            // We also scan a subset of the columns to trim off the hash column.
            // That column is accounted for in the layout, but omitted from the
            // grouping set.
            let num_groups_scan = self.grouping_set.len();
            state.groups.reset_for_write()?;
            let capacity = state.groups.write_capacity()?;
            let group_row_count = table.data.scan_groups_subset(
                scan_state,
                0..num_groups_scan,
                &mut state.groups.arrays,
                capacity,
            )?;

            if group_row_count == 0 {
                // No more groups, remove this table+state and try to move to
                // the next one.
                state.tables.pop().unwrap();
                state.scan_states.pop().unwrap();
                continue;
            }

            debug_assert_eq!(capacity, state.results.write_capacity()?);
            debug_assert!(group_row_count <= capacity);

            // Finalize aggregate states.
            state.results.reset_for_write()?;
            // SAFETY: All partitions should be scanning a disjoint set of
            // blocks across all aggregate tables. This partition should be the
            // only partition to compute the scanned pointers here.
            unsafe {
                self.layout.finalize_states(
                    scan_state.scanned_row_pointers_mut(),
                    &mut state.results.arrays,
                )?;
            }

            // Write groups to the output. This will write in the order of the
            // groups, but have them densely packed on the left.
            //
            // Groups not in the grouping set will have the output array modified to
            // be all nulls.
            let num_groups = self.groups.len();
            let mut group_idx = 0;
            for output_idx in 0..num_groups {
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
            for (src_idx, dest_idx) in (num_groups..(num_groups + num_aggs)).enumerate() {
                // Swapping should be fine here as well.
                state.results.swap_arrays(src_idx, (output, dest_idx))?;
            }

            // Append grouping values.
            for (idx, &grouping_val) in self.grouping_values.iter().enumerate() {
                let output_idx = num_groups + num_aggs + idx;
                let mut const_arr = Array::new_constant(
                    &DefaultBufferManager,
                    &ScalarValue::Int64(grouping_val),
                    group_row_count,
                )?;
                output.arrays[output_idx].swap(&mut const_arr)?;
            }

            output.set_num_rows(group_row_count)?;

            // We have results, get out of the loop.
            return Ok(());
        }
    }

    fn prepare_scan(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
    ) -> Result<()> {
        let partition_idx = match state {
            PartitionedHashTablePartitionState::ScanReady { partition_idx } => *partition_idx, // Continue.
            _ => {
                return Err(DbError::new(
                    "Partition in invalid state, cannot prepare scan",
                ));
            }
        };

        // Get a reference to all the global tables.
        let mut tables = Vec::with_capacity(op_state.final_tables.len());
        for final_table in &op_state.final_tables {
            let final_table = final_table.lock();
            match final_table.as_ref() {
                Some(table) => tables.push(table.clone()),
                None => return Err(DbError::new("Missing final table")),
            }
        }

        let num_partitions = tables.len();

        // Now initialize the scan states. All partitions will be scanning from
        // all tables, so we need to make sure we're scanning disjoint blocks.
        let row_scan_states: Vec<_> = tables
            .iter()
            .map(|table| {
                let block_indices = (0..table.data.num_row_blocks())
                    .filter(|block_idx| block_idx % num_partitions == partition_idx);

                RowScanState::new_partial_scan(block_indices)
            })
            .collect();

        // Batch buffers for scanning.
        let groups = Batch::new(self.group_types_no_hash_iter(), op_state.batch_size)?;
        let results = Batch::new(op_state.result_types.clone(), op_state.batch_size)?;

        *state = PartitionedHashTablePartitionState::Scanning(LocalScanningState {
            tables,
            scan_states: row_scan_states,
            groups,
            results,
        });

        Ok(())
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
