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
use crate::execution::operators::hash_aggregate::Aggregates;
use crate::execution::operators::hash_aggregate::grouping_value::compute_grouping_value;
use crate::execution::operators::hash_aggregate::hash_table::base::NO_GROUPS_HASH_VALUE;
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::util::cell::UnsafeSyncOnceCell;
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
    /// Batch for holding the grouping columns we're inserting into the table.
    /// Only stores columns for the grouping set.
    groups: Batch,
    /// Batch for holding aggregate inputs.
    inputs: Batch,
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
    /// Output types of the tables.
    ///
    /// Here for convenience.
    result_types: Vec<DataType>,
    /// Batch size to use when scanning.
    batch_size: usize,
    /// State initialized when we create the partition states.
    state: UnsafeSyncOnceCell<InitializedOperatorState>,
}

#[derive(Debug)]
struct InitializedOperatorState {
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
}

impl PartitionedHashTableOperatorState {
    fn get(&self) -> &InitializedOperatorState {
        unsafe { self.state.get().expect("state to have been initialized") }
    }
}

#[derive(Debug)]
struct FlushedTables {
    /// Tables that have been flush so far.
    tables: Vec<BaseHashTable>,
}

/// Hash table that partitions based on a row's hash.
#[derive(Debug)]
pub struct PartitionedHashTable {
    /// Layout of the aggregates.
    layout: AggregateLayout,
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    /// Complete set of groups, even if the group is not used in the grouping
    /// set.
    ///
    /// Used to output NULLs for groups not in this grouping set.
    groups: Vec<PhysicalColumnExpr>,
    /// Computed group values representing the null bitmask for eaching GROUPING
    /// function.
    grouping_values: Vec<i64>,
}

impl PartitionedHashTable {
    pub fn new(aggs: &Aggregates, grouping_set: BTreeSet<usize>) -> Self {
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

        let layout = AggregateLayout::new(group_types, aggs.aggregates.clone());

        PartitionedHashTable {
            layout,
            grouping_set,
            groups: aggs.groups.clone(),
            grouping_values,
        }
    }

    /// Create the global operator state.
    pub fn create_operator_state(
        &self,
        batch_size: usize,
    ) -> Result<PartitionedHashTableOperatorState> {
        let result_types: Vec<_> = self
            .layout
            .aggregates
            .iter()
            .map(|agg| agg.function.state.return_type.clone())
            .collect();

        Ok(PartitionedHashTableOperatorState {
            result_types,
            batch_size,
            state: UnsafeSyncOnceCell::new(), // Initialized when we create the partition states.
        })
    }

    /// Creates the partition states for the table.
    ///
    /// The partitions states will be initialized to a building state.
    pub fn create_partition_states(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        partitions: usize,
    ) -> Result<Vec<PartitionedHashTablePartitionState>> {
        // Update operator state.
        let init_state = InitializedOperatorState {
            flushed: (0..partitions)
                .map(|_| {
                    Mutex::new(FlushedTables {
                        tables: Vec::with_capacity(partitions),
                    })
                })
                .collect(),
            final_tables: (0..partitions).map(|_| Mutex::new(None)).collect(),
        };
        unsafe {
            op_state
                .state
                .set(init_state)
                .expect("op state to only be initialized once")
        };

        let states = (0..partitions)
            .map(|partition_idx| {
                // Note that this includes only the groups in the grouping set.
                //
                // In certain cases, this may result in an empty batch, e.g.
                // when dealing with rollups. The hash table has special logic
                // for when no group arrays are provided to ensure that all rows
                // still have a valid (and the same) hash.
                let groups = Batch::new(self.group_types_no_hash_iter(), op_state.batch_size)?;

                let agg_input_types: Vec<_> = self
                    .layout
                    .aggregates
                    .iter()
                    .flat_map(|agg| agg.columns.iter().map(|col| col.datatype.clone()))
                    .collect();
                let inputs = Batch::new(agg_input_types, op_state.batch_size)?;

                let tables = (0..partitions)
                    .map(|_| BaseHashTable::try_new(self.layout.clone(), op_state.batch_size))
                    .collect::<Result<Vec<_>>>()?;
                let states = tables.iter().map(|t| t.init_insert_state()).collect();

                Ok(PartitionedHashTablePartitionState::Building(
                    LocalBuildingState {
                        partition_idx,
                        partition_selector: PartitionSelector::new(partitions),
                        tables,
                        states,
                        groups,
                        inputs,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(states)
    }

    /// Returns an iterator of all group types except for the hash column.
    ///
    /// Used when creating batches for buffering user-provided group columns.
    fn group_types_no_hash_iter(&self) -> impl IntoExactSizeIterator<Item = DataType> + '_ {
        let types = &self.layout.groups.types;
        types.iter().take(types.len() - 1).cloned()
    }

    /// Similar to `insert_partition_local`, but with the input batches already
    /// containing the groups and inputs in the right order.
    ///
    /// Groups come first, followed by the aggregate inputs.
    ///
    /// The physical column expressions for the grouping set are not consulted.
    pub fn insert_partition_local_distinct_values(
        &self,
        state: &mut PartitionedHashTablePartitionState,
        agg_selection: &[usize],
        distinct_input: &mut Batch,
    ) -> Result<()> {
        let state = match state {
            PartitionedHashTablePartitionState::Building(building) => building,
            _ => {
                return Err(DbError::new(
                    "Partition in invalid state, cannot insert distinct",
                ));
            }
        };

        if agg_selection.is_empty() {
            return Ok(());
        }

        // TODO: Avoid doing this.
        //
        // Distinct inputs only affect a subset of aggregates, so the full batch
        // isn't needed. However the selection during the partitioning isn't as
        // robust as it needs to be, and tried to apply a selection on all
        // arrays.
        //
        // These resets are a temp workaround to ensure the selection has
        // something it can easily work with.
        state.groups.reset_for_write()?;
        state.inputs.reset_for_write()?;

        // Clone the group arrays.
        for idx in 0..self.grouping_set.len() {
            state.groups.clone_array_from(idx, (distinct_input, idx))?;
        }

        // Clone input arrays.

        let mut prev_sel = 0;
        let mut state_input_idx = 0;

        let mut input_idx = self.grouping_set.len();

        for &sel in agg_selection {
            while prev_sel != sel {
                state_input_idx += self.layout.aggregates[prev_sel].columns.len();
                prev_sel += 1;
            }

            for idx in 0..self.layout.aggregates[sel].columns.len() {
                state
                    .inputs
                    .clone_array_from(state_input_idx + idx, (distinct_input, input_idx))?;
                input_idx += 1;
            }
        }

        state.groups.set_num_rows(distinct_input.num_rows())?;
        state.inputs.set_num_rows(distinct_input.num_rows())?;

        self.insert_local(state, agg_selection)
    }

    /// Insert a batch into the local hash table.
    ///
    /// This will pull out the grouping columns according to this table's
    /// grouping set using physical column expressions, and insert into the hash
    /// table using those values.
    pub fn insert_partition_local(
        &self,
        state: &mut PartitionedHashTablePartitionState,
        agg_selection: &[usize],
        input: &mut Batch,
    ) -> Result<()> {
        let state = match state {
            PartitionedHashTablePartitionState::Building(building) => building,
            _ => return Err(DbError::new("Partition in invalid state, cannot insert")),
        };

        // Get both the group arrays, and the inputs to the aggregates.
        for (dest_idx, &src_idx) in self.grouping_set.iter().enumerate() {
            // Grouping set is relative to the groups, get the actual column
            // index from the group expression.
            let src_idx = self.groups[src_idx].idx;
            // Can't swap as other hash tables with different grouping sets
            // might have overlap with our grouping set.
            //
            // TODO: Since we insert into each table sequentially, we might be
            // able to get away with a swap, that revert the swap after the
            // insert. Need to determine if it's worth it.
            state.groups.clone_array_from(dest_idx, (input, src_idx))?;
        }

        for (dest_idx, src_idx) in self
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

        state.groups.set_num_rows(input.num_rows())?;
        state.inputs.set_num_rows(input.num_rows())?;

        self.insert_local(state, agg_selection)
    }

    /// Inserts a batch into this partition's local hash tables.
    ///
    /// This requires that `groups` and `inputs` have been populated on the
    /// local build state from the aggregate input.
    ///
    /// Internally partitions the input.
    fn insert_local(&self, state: &mut LocalBuildingState, agg_selection: &[usize]) -> Result<()> {
        let groups = &mut state.groups;
        let inputs = &mut state.inputs;

        debug_assert_eq!(groups.num_rows, inputs.num_rows);

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
        selector.reset_using_hashes(hashes);

        debug_assert_eq!(state.tables.len(), state.states.len());

        // Now insert into each partition table.
        for (partition_idx, (table, table_state)) in
            state.tables.iter_mut().zip(&mut state.states).enumerate()
        {
            let row_sel = state
                .partition_selector
                .row_indices_for_partition(partition_idx);

            if row_sel.len() == 0 {
                // Nothing to input for this partition.
                continue;
            }

            let mut groups = groups.clone()?;
            groups.select(row_sel.clone())?;

            let mut inputs = inputs.clone()?;
            inputs.select(row_sel.clone())?;

            let mut hashes_arr = hashes_arr.clone()?;
            hashes_arr.select(&DefaultBufferManager, row_sel)?;

            table.insert_with_hashes(table_state, agg_selection, &groups, &inputs, &hashes_arr)?;
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

        debug_assert_eq!(building.tables.len(), op_state.get().flushed.len());

        for (partition_idx, table) in building.tables.drain(..).enumerate() {
            let mut flushed = op_state.get().flushed[partition_idx].lock();
            flushed.tables.push(table);
        }

        *state = PartitionedHashTablePartitionState::MergeReady {
            partition_idx: building.partition_idx,
        };

        Ok(())
    }

    /// Merges the global hash tables that this partition is responsible for.
    ///
    /// Should be called once per partition.
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
        let mut flushed = op_state.get().flushed[partition_idx].lock();
        if flushed.tables.len() != op_state.get().flushed.len() {
            // Means not all partitions flushed.
            return Err(DbError::new(
                "Attempted to merge into final table, but some tables missing",
            )
            .with_field("expected", op_state.get().flushed.len())
            .with_field("got", flushed.tables.len()));
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
        let mut final_table = op_state.get().final_tables[partition_idx].lock();
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
        let mut tables = Vec::with_capacity(op_state.get().final_tables.len());
        for final_table in &op_state.get().final_tables {
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
#[derive(Debug, Clone)]
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

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::expr::physical::PhysicalAggregateExpression;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};

    #[test]
    fn partition_selector_single_partition() {
        let mut selector = PartitionSelector::new(1);
        selector.reset_using_hashes(&[55, 66, 77, 88, 99]);

        let rows: Vec<_> = selector.row_indices_for_partition(0).collect();
        let expected = vec![0, 1, 2, 3, 4];
        assert_eq!(expected, rows);
    }

    #[test]
    fn partition_selector_multiple_partitions() {
        // Sanity check to ensure we're distributing over available partitions.
        let mut rng = rand_chacha::ChaCha20Rng::seed_from_u64(84);
        let hashes: Vec<u64> = (0..10).map(|_| rng.random()).collect();

        println!("hashes: {hashes:?}");

        let mut selector = PartitionSelector::new(4);
        selector.reset_using_hashes(&hashes);

        let rows0: Vec<_> = selector.row_indices_for_partition(0).collect();
        let rows1: Vec<_> = selector.row_indices_for_partition(1).collect();
        let rows2: Vec<_> = selector.row_indices_for_partition(2).collect();
        let rows3: Vec<_> = selector.row_indices_for_partition(3).collect();

        assert_eq!(vec![3, 6, 8, 9], rows0);
        assert_eq!(Vec::<usize>::new(), rows1);
        assert_eq!(vec![0, 2, 4], rows2);
        assert_eq!(vec![1, 5, 7], rows3);
    }

    #[test]
    fn single_insert_merge_scan() {
        // GROUP     (col0): Utf8
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();

        let aggs = Aggregates {
            groups: vec![(0, DataType::Utf8).into()],
            grouping_functions: Vec::new(),
            aggregates: vec![PhysicalAggregateExpression::new(
                sum_agg,
                [(1, DataType::Int64)],
            )],
        };

        let grouping_set: BTreeSet<usize> = [0].into();
        let table = PartitionedHashTable::new(&aggs, grouping_set);
        let op_state = table.create_operator_state(16).unwrap();
        let mut part_states = table.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut input = generate_batch!(["a", "b", "c", "a"], [1_i64, 2, 3, 4]);
        table
            .insert_partition_local(&mut part_states[0], &[0], &mut input)
            .unwrap();

        table.flush(&op_state, &mut part_states[0]).unwrap();
        table.merge_global(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::Utf8, DataType::Int64], 16).unwrap();
        table
            .scan(&op_state, &mut part_states[0], &mut out)
            .unwrap();

        let expected = generate_batch!(["a", "b", "c"], [5_i64, 2, 3]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn single_insert_merge_scan_grouping_set() {
        // Two grouping columns, but hash table has grouping set only
        // referencing the last grouping column.

        // GROUP_0     (col0): Utf8
        // AGG_INPUT   (col1): Int64
        // GROUP_1     (col2): Utf8
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::Int64).into()],
        )
        .unwrap();

        let aggs = Aggregates {
            groups: vec![(0, DataType::Utf8).into(), (2, DataType::Utf8).into()],
            grouping_functions: Vec::new(),
            aggregates: vec![PhysicalAggregateExpression::new(
                sum_agg,
                [(1, DataType::Int64)],
            )],
        };

        let grouping_set: BTreeSet<usize> = [1].into(); // '1' relative to groups (real column index of 2)
        let table = PartitionedHashTable::new(&aggs, grouping_set);
        let mut op_state = table.create_operator_state(16).unwrap();
        let mut part_states = table.create_partition_states(&mut op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut input = generate_batch!(
            ["a", "b", "c", "a"],
            [1_i64, 2, 3, 4],
            ["gg", "ff", "gg", "ff"]
        );
        table
            .insert_partition_local(&mut part_states[0], &[0], &mut input)
            .unwrap();

        table.flush(&op_state, &mut part_states[0]).unwrap();
        table.merge_global(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::Utf8, DataType::Utf8, DataType::Int64], 16).unwrap();
        table
            .scan(&op_state, &mut part_states[0], &mut out)
            .unwrap();

        let expected = generate_batch!([None as Option<&str>, None], ["gg", "ff"], [4_i64, 6]);
        assert_batches_eq(&expected, &out);
    }
}
