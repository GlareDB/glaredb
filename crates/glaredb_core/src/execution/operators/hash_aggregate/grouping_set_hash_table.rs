use std::collections::BTreeSet;
use std::sync::Arc;

use glaredb_error::{DbError, OptionExt, Result};
use parking_lot::Mutex;

use super::Aggregates;
use super::hash_table::base::{BaseHashTable, BaseHashTableInsertState};
use crate::arrays::array::Array;
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_scan::RowScanState;
use crate::arrays::scalar::ScalarValue;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::execution::operators::hash_aggregate::grouping_value::compute_grouping_value;
use crate::execution::operators::util::delayed_count::DelayedPartitionCount;
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::util::iter::IntoExactSizeIterator;

#[derive(Debug)]
pub struct GroupingSetPartitionState {
    /// Index of this partition.
    partition_idx: usize,
    /// Inner partition state.
    inner: PartitionState,
}

#[derive(Debug)]
enum PartitionState {
    /// Partition is currently building its partition-local table.
    Building(GroupingSetBuildPartitionState),
    /// This partition is ready for scanning.
    ///
    /// Wait until all other partitions have merged their table before starting
    /// to scan.
    ScanReady,
    /// Partitions is actively scanning.
    Scanning(GroupingSetScanPartitionState),
}

#[derive(Debug)]
pub struct GroupingSetBuildPartitionState {
    /// Insert state into the local hash table.
    insert_state: BaseHashTableInsertState,
    /// The actual hash table.
    hash_table: Box<BaseHashTable>,
    /// Batch for holding the grouping columns we're inserting into the table.
    /// Only stores columns for the grouping set.
    groups: Batch,
    /// Batch for holding aggregate inputs.
    inputs: Batch,
}

#[derive(Debug)]
pub struct GroupingSetScanPartitionState {
    /// The final hash table.
    hash_table: Arc<BaseHashTable>,
    /// Scan state for scanning the hash table.
    ///
    /// Initialized to scan only a subset of blocks that only this partition will
    /// be scanning.
    scan_state: RowScanState,
    /// Batch for reading groups from the hash table.
    groups: Batch,
    /// Batch for reading the aggregate states from the hash table.
    results: Batch,
}

#[derive(Debug)]
pub struct GroupingSetOperatorState {
    /// Batch size to use when scanning.
    batch_size: usize,
    /// Building or scanning state.
    inner: Mutex<OperatorState>,
}

#[derive(Debug)]
enum OperatorState {
    Building(HashTableBuildingOperatorState),
    Scanning(HashTableScanningOperatorState),
}

#[derive(Debug)]
pub struct HashTableBuildingOperatorState {
    /// Number of partitions that are building.
    ///
    /// Initialized when we create the partition-local build states.
    partitions: Option<usize>,
    /// Remaining inputs we're waiting on to finish.
    ///
    /// Initialized when we create the partition-local build states.
    remaining: DelayedPartitionCount,
    /// Hash tables from each partition.
    ///
    /// Once we have all hash tables, we can merge them into the global table.
    flushed: Vec<BaseHashTable>,
}

#[derive(Debug)]
pub struct HashTableScanningOperatorState {
    /// Number of partitions that are scanning.
    partitions: usize,
    /// The final hash table.
    hash_table: Arc<BaseHashTable>,
    /// Output types of the table.
    ///
    /// Here for convenience.
    result_types: Vec<DataType>,
}

/// Wrapper around a hash table for dealing with a single grouping set.
///
/// Output batch layout: [GROUP_VALS, AGG_RESULTS, GROUPING_VALS]
#[derive(Debug)]
pub struct GroupingSetHashTable {
    layout: AggregateLayout,
    /// The grouping set for this table.
    grouping_set: BTreeSet<usize>,
    groups: Vec<PhysicalColumnExpr>,
    /// Computed group values representing the null bitmask for eaching GROUPING
    /// function.
    grouping_values: Vec<i64>,
}

impl GroupingSetHashTable {
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

        GroupingSetHashTable {
            layout,
            grouping_set,
            groups: aggs.groups.clone(),
            grouping_values,
        }
    }

    /// Create the global operator state.
    pub fn create_operator_state(&self, batch_size: usize) -> Result<GroupingSetOperatorState> {
        let op_state = GroupingSetOperatorState {
            batch_size,
            inner: Mutex::new(OperatorState::Building(HashTableBuildingOperatorState {
                partitions: None,
                remaining: DelayedPartitionCount::uninit(),
                flushed: Vec::new(),
            })),
        };

        Ok(op_state)
    }

    /// Creates the partition states for the table.
    ///
    /// The partitions states will be initialized to a building state.
    pub fn create_partition_states(
        &self,
        op_state: &GroupingSetOperatorState,
        partitions: usize,
    ) -> Result<Vec<GroupingSetPartitionState>> {
        let mut inner = op_state.inner.lock();
        match &mut *inner {
            OperatorState::Building(state) => {
                state.partitions = Some(partitions);
                state.remaining.set(partitions)?;
                state.flushed.reserve(partitions);
            }
            other => panic!("grouping set operator state in invalid state: {other:?}"),
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

                let table = BaseHashTable::try_new(self.layout.clone(), op_state.batch_size)?;
                let insert_state = table.init_insert_state();

                Ok(GroupingSetPartitionState {
                    partition_idx,
                    inner: PartitionState::Building(GroupingSetBuildPartitionState {
                        insert_state,
                        hash_table: Box::new(table),
                        groups,
                        inputs,
                    }),
                })
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

    /// Similar to `insert`, but with the input batches already containing the
    /// groups and inputs in the right order.
    ///
    /// Groups come first, followed by the aggregate inputs.
    ///
    /// The physical column expressions for the grouping set are not consulted.
    pub fn insert_for_distinct_local(
        &self,
        state: &mut GroupingSetPartitionState,
        agg_selection: &[usize],
        input: &mut Batch,
    ) -> Result<()> {
        let state = match &mut state.inner {
            PartitionState::Building(state) => state,
            _ => return Err(DbError::new("Partition-local table already finished")),
        };

        if agg_selection.is_empty() {
            return Ok(());
        }

        // Clone the group arrays.
        for idx in 0..self.grouping_set.len() {
            state.groups.clone_array_from(idx, (input, idx))?;
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
                    .clone_array_from(state_input_idx + idx, (input, input_idx))?;
                input_idx += 1;
            }
        }

        state.groups.set_num_rows(input.num_rows())?;
        state.inputs.set_num_rows(input.num_rows())?;

        state.hash_table.insert(
            &mut state.insert_state,
            agg_selection,
            0..state.groups.num_rows,
            &state.groups,
            &state.inputs,
        )?;

        Ok(())
    }

    /// Insert a batch into the local hash table.
    ///
    /// This will pull out the grouping columns according to this table's
    /// grouping set using physical column expressions, and insert into the hash
    /// table using those values.
    pub fn insert_input_local(
        &self,
        state: &mut GroupingSetPartitionState,
        agg_selection: &[usize],
        input: &mut Batch,
    ) -> Result<()> {
        let state = match &mut state.inner {
            PartitionState::Building(state) => state,
            _ => return Err(DbError::new("Partition-local table already finished")),
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

        state.groups.set_num_rows(input.num_rows())?;
        state.inputs.set_num_rows(input.num_rows())?;

        state.hash_table.insert(
            &mut state.insert_state,
            agg_selection,
            0..state.groups.num_rows,
            &state.groups,
            &state.inputs,
        )?;

        Ok(())
    }

    /// Flushes the local hash table to the global state.
    ///
    /// Returns `true` if this was the last partition that needed to be flushed,
    /// indicating we should merge into the global table.
    pub fn flush(
        &self,
        op_state: &GroupingSetOperatorState,
        state: &mut GroupingSetPartitionState,
    ) -> Result<bool> {
        let mut inner = op_state.inner.lock();
        match &mut *inner {
            OperatorState::Building(building) => {
                let part_state = std::mem::replace(&mut state.inner, PartitionState::ScanReady);
                let part_table = match part_state {
                    PartitionState::Building(building) => building.hash_table, // Just need the hash table, drop everything else.
                    _ => return Err(DbError::new("Expected partition state to be building")),
                };

                building.flushed.push(*part_table);
                let remaining = building.remaining.dec_by_one()?;

                Ok(remaining == 0)
            }
            _ => Err(DbError::new(
                "Operator hash table not in building state, cannot flush",
            )),
        }
    }

    /// Merges all flushed partitions tables into the global hash table.
    ///
    /// This should be called by one partition, and not in conjunction with
    /// scanning as this is an expensive operation happening within a lock.
    pub fn merge_flushed(&self, op_state: &GroupingSetOperatorState) -> Result<()> {
        let mut inner = op_state.inner.lock();
        match &mut *inner {
            OperatorState::Building(building) => {
                if building.remaining.current()? != 0 {
                    return Err(DbError::new(
                        "Cannot merge with outstanding partitions still buidling",
                    ));
                }

                let mut drain = building.flushed.drain(..);

                // First table will be our global table.
                let mut global = drain.next().required("at least one partition")?;
                let mut insert_state = global.init_insert_state();

                for mut table in drain {
                    global.merge_from(
                        &mut insert_state,
                        0..self.layout.aggregates.len(),
                        &mut table,
                    )?;
                }

                let partitions = building.partitions.required("total partition count")?;
                let result_types = self
                    .layout
                    .aggregates
                    .iter()
                    .map(|agg| agg.function.state.return_type.clone())
                    .collect();

                *inner = OperatorState::Scanning(HashTableScanningOperatorState {
                    partitions,
                    hash_table: Arc::new(global),
                    result_types,
                });

                Ok(())
            }
            _ => Err(DbError::new(
                "Operator hash table not in building state, cannot merge",
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
    pub fn scan(
        &self,
        op_state: &GroupingSetOperatorState,
        state: &mut GroupingSetPartitionState,
        output: &mut Batch,
    ) -> Result<()> {
        let state = match &mut state.inner {
            PartitionState::Scanning(state) => state,
            PartitionState::ScanReady => {
                // Prep scan.
                let mut inner = op_state.inner.lock();
                match &mut *inner {
                    OperatorState::Scanning(scan_op) => {
                        let groups =
                            Batch::new(self.group_types_no_hash_iter(), op_state.batch_size)?;
                        let results =
                            Batch::new(scan_op.result_types.clone(), op_state.batch_size)?;

                        let partitions = scan_op.partitions;

                        // Init this row scan state with a subset of blocks
                        // to scan.
                        let row_scan_state = RowScanState::new_partial_scan(
                            (0..scan_op.hash_table.data.num_row_blocks())
                                .filter(|block_idx| block_idx % partitions == state.partition_idx),
                        );

                        state.inner = PartitionState::Scanning(GroupingSetScanPartitionState {
                            hash_table: scan_op.hash_table.clone(),
                            groups,
                            results,
                            scan_state: row_scan_state,
                        })
                    }
                    _ => return Err(DbError::new("Attempted to scan before global table ready")),
                }

                match &mut state.inner {
                    PartitionState::Scanning(state) => state,
                    _ => unreachable!(),
                }
            }
            _ => return Err(DbError::new("Attempted to scan while still in build state")),
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
        state.results.reset_for_write()?;
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::physical::PhysicalAggregateExpression;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};

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
        let table = GroupingSetHashTable::new(&aggs, grouping_set);
        let op_state = table.create_operator_state(16).unwrap();
        let mut part_states = table.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut input = generate_batch!(["a", "b", "c", "a"], [1_i64, 2, 3, 4]);
        table
            .insert_input_local(&mut part_states[0], &[0], &mut input)
            .unwrap();

        let merge_ready = table.flush(&op_state, &mut part_states[0]).unwrap();
        assert!(merge_ready);
        table.merge_flushed(&op_state).unwrap();

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
        let table = GroupingSetHashTable::new(&aggs, grouping_set);
        let mut op_state = table.create_operator_state(16).unwrap();
        let mut part_states = table.create_partition_states(&mut op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut input = generate_batch!(
            ["a", "b", "c", "a"],
            [1_i64, 2, 3, 4],
            ["gg", "ff", "gg", "ff"]
        );
        table
            .insert_input_local(&mut part_states[0], &[0], &mut input)
            .unwrap();

        let merge_ready = table.flush(&op_state, &mut part_states[0]).unwrap();
        assert!(merge_ready);
        table.merge_flushed(&op_state).unwrap();

        let mut out = Batch::new([DataType::Utf8, DataType::Utf8, DataType::Int64], 16).unwrap();
        table
            .scan(&op_state, &mut part_states[0], &mut out)
            .unwrap();

        let expected = generate_batch!([None as Option<&str>, None], ["gg", "ff"], [4_i64, 6]);
        assert_batches_eq(&expected, &out);
    }
}
