use std::collections::BTreeSet;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, atomic};

use glaredb_error::{DbError, Result};

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
    Initializing(LocalInitializingState),
    /// Partition is building its local hash tables.
    Building(LocalBuildingState),
    /// Partition has flushed its local hash tables to the operator state.
    MergeReady {
        partition_idx: usize,
    },
    /// Partition has merged its tables.
    ScanReady {
        partition_idx: usize,
    },
    /// Partition is scanning.
    Scanning(LocalScanningState),
    /// Unreachable state.
    Uninit,
}

#[derive(Debug)]
pub struct LocalInitializingState {
    partition_idx: usize,
    partition_selector: PartitionSelector,
    groups: Batch,
    inputs: Batch,
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
    flushed: Vec<FlushedTables>,
    /// The final aggregate tables.
    final_tables: Vec<FinalTable>,

    /// Remaining partitions that still need to flush their tables.
    ///
    /// This is needed for atomic fencing to ensure the unsynchronized puts to
    /// the tables vec is visible once the partition starts to merge.
    remaining_flushers: AtomicUsize,
    /// Remaining partitions for merging the tables they're responsible for
    /// merging.
    ///
    /// Also needed for fencing.
    remaining_mergers: AtomicUsize,
}

impl PartitionedHashTableOperatorState {
    fn partition_count(&self) -> usize {
        self.get().final_tables.len()
    }

    fn get(&self) -> &InitializedOperatorState {
        unsafe { self.state.get().expect("state to have been initialized") }
    }
}

#[derive(Debug)]
struct FinalTable {
    /// The final table, set only once the partition has merged all tables it's
    /// responsible for.
    table: UnsafeSyncOnceCell<Arc<BaseHashTable>>,
}

/// Holds the intermediate tables that's been flushed for each partition.
#[derive(Debug)]
struct FlushedTables {
    /// Flushed tables written by each partition.
    ///
    /// When flushing tables, each partition should write to a unique index as
    /// specified by its partition idx.
    ///
    /// This is exact-sized.
    tables: Vec<UnsafeSyncOnceCell<BaseHashTable>>,
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
    pub fn try_new(aggs: &Aggregates, grouping_set: BTreeSet<usize>) -> Result<Self> {
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
            .chain([DataType::uint64()]);

        let layout = AggregateLayout::try_new(group_types, aggs.aggregates.clone())?;

        Ok(PartitionedHashTable {
            layout,
            grouping_set,
            groups: aggs.groups.clone(),
            grouping_values,
        })
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
                .map(|_| FlushedTables {
                    tables: (0..partitions).map(|_| UnsafeSyncOnceCell::new()).collect(),
                })
                .collect(),
            final_tables: (0..partitions)
                .map(|_| FinalTable {
                    table: UnsafeSyncOnceCell::new(),
                })
                .collect(),
            remaining_flushers: AtomicUsize::new(partitions),
            remaining_mergers: AtomicUsize::new(partitions),
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

                Ok(PartitionedHashTablePartitionState::Initializing(
                    LocalInitializingState {
                        partition_idx,
                        partition_selector: PartitionSelector::new(partitions),
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
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
        agg_selection: &[usize],
        distinct_input: &mut Batch,
    ) -> Result<()> {
        self.prepare_build_maybe(op_state, state)?;
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
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
        agg_selection: &[usize],
        input: &mut Batch,
    ) -> Result<()> {
        self.prepare_build_maybe(op_state, state)?;
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
        let mut hashes_arr =
            Array::new(&DefaultBufferManager, DataType::uint64(), groups.num_rows)?;
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
            let row_sel = &selector.buckets[partition_idx];
            if row_sel.is_empty() {
                // Nothing to input for this partition.
                continue;
            }

            let mut groups = groups.clone()?;
            groups.select(row_sel.iter().copied())?;

            let mut inputs = inputs.clone()?;
            inputs.select(row_sel.iter().copied())?;

            let mut hashes_arr = hashes_arr.clone()?;
            hashes_arr.select(&DefaultBufferManager, row_sel.iter().copied())?;

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
        self.prepare_build_maybe(op_state, state)?; // We may be in the initializing state if this partition didn't actually insert anything.
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
            // Get the flushed table for the _output_ partition.
            let flushed = &op_state.get().flushed[partition_idx];

            // Write our local table using this partition's index.
            //
            // SAFETY: Each partition state was initialized with a unique
            // partition index. Our set should not conflict with some other
            // partition trying to set.
            //
            // When doing the final merge, higher level synchronization ensures
            // that we have no partitions actively flushing their tables, so
            // this shouldn't conflict with `get` during merging.
            unsafe {
                flushed.tables[building.partition_idx]
                    .set(table)
                    .expect("partition table to have only been initialized once")
            };
        }

        // `Release` to ensure visibility of the above tables when merging.
        let prev = op_state
            .get()
            .remaining_flushers
            .fetch_sub(1, atomic::Ordering::Release);
        assert_ne!(
            0, prev,
            "Atomic remaining count for flushers in partitioned hash table must not go negative"
        );

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
        // All partitions should have flushed their tables by this point.

        let flushed = &op_state.get().flushed[partition_idx];
        // This atomic access is for the fence, ensure we see all unsynchronized
        // flushed tables for this partition.
        let remaining = op_state
            .get()
            .remaining_flushers
            .load(atomic::Ordering::Acquire);
        if remaining != 0 {
            // Means not all partitions flushed.
            return Err(DbError::new(
                "Attempted to merge into final table, but some tables missing",
            )
            .with_field("remaining", remaining));
        }

        // Pick arbitrary table to be the global table we merge into.
        assert!(
            !flushed.tables.is_empty(),
            "Must have at least one flushed table"
        );
        // SAFETY: Each partition is working on their own set of flushed tables.
        //
        // This partitions should be the only one accessing this set of flushed
        // tables.
        let mut global =
            unsafe { flushed.tables[0].take() }.expect("first table to have been initialized");
        let mut insert_state = global.init_insert_state();

        for other in &flushed.tables[1..] {
            // SAFETY: Same as above, this partition should be the only
            // partition touching these sets of tables.
            let other = unsafe { other.get_mut() }.expect("other table to have been initialized");

            // Merge it in.
            global.merge_from(&mut insert_state, 0..self.layout.aggregates.len(), other)?;
        }

        // Now put it in the global state.
        let final_table = &op_state.get().final_tables[partition_idx];
        // SAFETY: As above, this partition should be the only one initialing
        // this final table.
        unsafe { final_table.table.set(Arc::new(global)) }
            .expect("final table to not have been initialized");

        // `Release` fence so other threads see above.
        let prev = op_state
            .get()
            .remaining_mergers
            .fetch_sub(1, atomic::Ordering::Release);
        assert_ne!(
            0, prev,
            "Atomic count for remaining mergers must not go negative"
        );

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

    fn prepare_build_maybe(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
    ) -> Result<()> {
        if let PartitionedHashTablePartitionState::Initializing(_) = state {
            self.prepare_build(op_state, state)?;
        }
        Ok(())
    }

    /// Transitions the partition state form 'initializing' to building.
    ///
    /// This is responsible for allocating the partition-local hash tables. We do
    /// this as a separate step during execution to avoid trying allocate all tables across
    /// all partitions in a single thread.
    fn prepare_build(
        &self,
        op_state: &PartitionedHashTableOperatorState,
        state: &mut PartitionedHashTablePartitionState,
    ) -> Result<()> {
        let orig = std::mem::replace(state, PartitionedHashTablePartitionState::Uninit);
        let initializing = match orig {
            PartitionedHashTablePartitionState::Initializing(init) => init,
            _ => {
                return Err(DbError::new(
                    "Partition in invalid state, cannot prepare build",
                ));
            }
        };

        let tables = (0..op_state.partition_count())
            .map(|_| BaseHashTable::try_new(self.layout.clone(), op_state.batch_size))
            .collect::<Result<Vec<_>>>()?;
        let states = tables.iter().map(|t| t.init_insert_state()).collect();

        *state = PartitionedHashTablePartitionState::Building(LocalBuildingState {
            partition_idx: initializing.partition_idx,
            partition_selector: initializing.partition_selector,
            tables,
            states,
            groups: initializing.groups,
            inputs: initializing.inputs,
        });

        Ok(())
    }

    /// Transitions the partition state from 'scan ready' to 'scanning'.
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

        // Atomic for the fence.
        let remaining = op_state
            .get()
            .remaining_mergers
            .load(atomic::Ordering::Acquire);
        if remaining != 0 {
            // Means not all partitions merged.
            return Err(DbError::new(
                "Attempted to read final tables before all partitions finished merging",
            )
            .with_field("remaining", remaining));
        }

        // Get a reference to all the global tables.
        let mut tables = Vec::with_capacity(op_state.get().final_tables.len());
        for final_table in &op_state.get().final_tables {
            // SAFETY: We should only get here if all final tables have been
            // created.
            //
            // The `get` may be happening concurrently with other threads, but
            // that's fine.
            let table = unsafe { final_table.table.get() }
                .expect("table to have been set")
                .clone();

            tables.push(table);
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
    /// For each row, which partition it belongs to.
    partition_indices: Vec<usize>,
    /// How many rows in each partition.
    counts: Vec<usize>,
    /// Row‐index buckets per partition.
    buckets: Vec<Vec<usize>>,
}

impl PartitionSelector {
    pub fn new(partition_count: usize) -> Self {
        Self {
            partition_indices: Vec::new(),
            counts: vec![0; partition_count],
            buckets: vec![Vec::new(); partition_count],
        }
    }

    /// Resets the selector state by recomputing partitoin indices using the
    /// provided hashes.
    fn reset_using_hashes(&mut self, hashes: &[u64]) {
        let pcount = self.counts.len();
        let counts = &mut self.counts;
        let buckets = &mut self.buckets;

        // Clear old state
        self.partition_indices.clear();
        counts.fill(0);
        for bucket in buckets.iter_mut() {
            bucket.clear();
        }

        // Compute partition indices.
        for &hash in hashes {
            let p = partition(hash, pcount);
            self.partition_indices.push(p);
            counts[p] += 1;
        }

        // Reserve buckets.
        for (partition, &count) in counts.iter().enumerate() {
            let bucket = &mut buckets[partition];

            let additional = count.saturating_sub(bucket.capacity());
            bucket.reserve(additional);
        }

        // Write to buckets.
        for (i, &partition) in self.partition_indices.iter().enumerate() {
            buckets[partition].push(i);
        }
    }
}

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

        let expected = vec![0, 1, 2, 3, 4];
        assert_eq!(expected, selector.buckets[0]);
    }

    #[test]
    fn partition_selector_multiple_partitions() {
        // Sanity check to ensure we're distributing over available partitions.
        let mut rng = rand_chacha::ChaCha20Rng::seed_from_u64(84);
        let hashes: Vec<u64> = (0..10).map(|_| rng.random()).collect();

        let mut selector = PartitionSelector::new(4);
        selector.reset_using_hashes(&hashes);

        assert_eq!(vec![3, 6, 8, 9], selector.buckets[0]);
        assert_eq!(Vec::<usize>::new(), selector.buckets[1]);
        assert_eq!(vec![0, 2, 4], selector.buckets[2]);
        assert_eq!(vec![1, 5, 7], selector.buckets[3]);
    }

    #[test]
    fn single_insert_merge_scan() {
        // GROUP     (col0): Utf8
        // AGG_INPUT (col1): Int64
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 1), DataType::int64())],
        )
        .unwrap();

        let aggs = Aggregates {
            groups: vec![(0, DataType::utf8()).into()],
            grouping_functions: Vec::new(),
            aggregates: vec![PhysicalAggregateExpression::new(
                sum_agg,
                [(1, DataType::int64())],
            )],
        };

        let grouping_set: BTreeSet<usize> = [0].into();
        let table = PartitionedHashTable::try_new(&aggs, grouping_set).unwrap();
        let op_state = table.create_operator_state(16).unwrap();
        let mut part_states = table.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut input = generate_batch!(["a", "b", "c", "a"], [1_i64, 2, 3, 4]);
        table
            .insert_partition_local(&op_state, &mut part_states[0], &[0], &mut input)
            .unwrap();

        table.flush(&op_state, &mut part_states[0]).unwrap();
        table.merge_global(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::utf8(), DataType::int64()], 16).unwrap();
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
            vec![expr::column((0, 1), DataType::int64())],
        )
        .unwrap();

        let aggs = Aggregates {
            groups: vec![(0, DataType::utf8()).into(), (2, DataType::utf8()).into()],
            grouping_functions: Vec::new(),
            aggregates: vec![PhysicalAggregateExpression::new(
                sum_agg,
                [(1, DataType::int64())],
            )],
        };

        let grouping_set: BTreeSet<usize> = [1].into(); // '1' relative to groups (real column index of 2)
        let table = PartitionedHashTable::try_new(&aggs, grouping_set).unwrap();
        let mut op_state = table.create_operator_state(16).unwrap();
        let mut part_states = table.create_partition_states(&mut op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut input = generate_batch!(
            ["a", "b", "c", "a"],
            [1_i64, 2, 3, 4],
            ["gg", "ff", "gg", "ff"]
        );
        table
            .insert_partition_local(&op_state, &mut part_states[0], &[0], &mut input)
            .unwrap();

        table.flush(&op_state, &mut part_states[0]).unwrap();
        table.merge_global(&op_state, &mut part_states[0]).unwrap();

        let mut out =
            Batch::new([DataType::utf8(), DataType::utf8(), DataType::int64()], 16).unwrap();
        table
            .scan(&op_state, &mut part_states[0], &mut out)
            .unwrap();

        let expected = generate_batch!([None as Option<&str>, None], ["gg", "ff"], [4_i64, 6]);
        assert_batches_eq(&expected, &out);
    }
}
