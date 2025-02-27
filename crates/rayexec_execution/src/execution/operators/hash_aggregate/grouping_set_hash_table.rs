use std::collections::BTreeSet;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::aggregate_hash_table::{AggregateHashTable, AggregateHashTableInsertState};
use super::Aggregates;
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::row::aggregate_layout::AggregateLayout;
use crate::arrays::row::row_scan::RowScanState;
use crate::arrays::scalar::BorrowedScalarValue;
use crate::execution::operators::hash_aggregate::grouping_value::compute_grouping_value;
use crate::expr::physical::column_expr::PhysicalColumnExpr;

#[derive(Debug)]
pub struct GroupingSetBuildPartitionState {
    /// If this table has been merged into the global table.
    finished: bool,
    /// Insert state into the local hash table.
    insert_state: AggregateHashTableInsertState,
    /// The actual hash table.
    hash_table: AggregateHashTable,
    /// Batch for holding the grouping columns we're inserting into the table.
    /// Only stores columns for the grouping set.
    groups: Batch,
    /// Batch for holding aggregate inputs.
    inputs: Batch,
}

#[derive(Debug)]
pub struct GroupingSetScanPartitionState {
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
pub enum GroupingSetOperatorState {
    Building(HashTableBuildingOperatorState),
    Scanning(HashTableScanningOperatorState),
    Uninit,
}

#[derive(Debug)]
pub struct HashTableBuildingOperatorState {
    /// Remaining inputs we're waiting on to finish.
    remaining: usize,
    /// The global hash table.
    hash_table: AggregateHashTable,
}

#[derive(Debug)]
pub struct HashTableScanningOperatorState {
    /// Scan states for each partition.
    ///
    /// This will be initialized to the number of partitions, with each
    /// containing a row scan state for indicating which rows that partition
    /// will be scanning.
    scan_states: Vec<GroupingSetScanPartitionState>,
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

        let layout = AggregateLayout::new(group_types, aggs.aggregates.clone());

        GroupingSetHashTable {
            layout,
            grouping_set,
            groups: aggs.groups.clone(),
            grouping_values,
            partitions,
            batch_size,
        }
    }

    pub fn init_states(
        &self,
    ) -> Result<(
        GroupingSetOperatorState,
        Vec<GroupingSetBuildPartitionState>,
    )> {
        let agg_hash_table = AggregateHashTable::try_new(self.layout.clone(), self.batch_size)?;
        let op_state = GroupingSetOperatorState::Building(HashTableBuildingOperatorState {
            remaining: self.partitions,
            hash_table: agg_hash_table,
        });

        let build_states = (0..self.partitions)
            .map(|_| {
                // Note that this includes only the groups in the grouping set.
                let groups = Batch::new(self.group_types_no_hash_iter(), self.batch_size)?;
                let agg_input_types: Vec<_> = self
                    .layout
                    .aggregates
                    .iter()
                    .flat_map(|agg| agg.columns.iter().map(|col| col.datatype.clone()))
                    .collect();
                let inputs = Batch::new(agg_input_types, self.batch_size)?;

                let table = AggregateHashTable::try_new(self.layout.clone(), self.batch_size)?;
                let insert_state = table.init_insert_state();

                Ok(GroupingSetBuildPartitionState {
                    finished: false,
                    insert_state,
                    hash_table: table,
                    groups,
                    inputs,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((op_state, build_states))
    }

    /// Returns an iterator of all group types except for the hash column.
    ///
    /// Used when creating batches for buffering user-provided group columns.
    fn group_types_no_hash_iter(&self) -> impl IntoExactSizeIterator<Item = DataType> + '_ {
        let types = &self.layout.groups.types;
        types.iter().take(types.len() - 1).cloned()
    }

    /// Insert a batch into the hash table.
    ///
    /// This will pull out the grouping columns according to this table's
    /// grouping set, and insert into the hash table using those values.
    pub fn insert(
        &self,
        state: &mut GroupingSetBuildPartitionState,
        input: &mut Batch,
    ) -> Result<()> {
        if state.finished {
            return Err(RayexecError::new("Partition-local table already finished"));
        }

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
        op_state: &mut GroupingSetOperatorState,
        state: &mut GroupingSetBuildPartitionState,
    ) -> Result<bool> {
        if state.finished {
            return Err(RayexecError::new("State already finished"));
        }

        match &mut *op_state {
            GroupingSetOperatorState::Building(building) => {
                building
                    .hash_table
                    .merge_from(&mut state.insert_state, &mut state.hash_table)?;

                building.remaining -= 1;

                if building.remaining == 0 {
                    // We were the last partition to merge, generate all
                    // necessary scan states.
                    let state = std::mem::replace(&mut *op_state, GroupingSetOperatorState::Uninit);
                    let state = match state {
                        GroupingSetOperatorState::Building(state) => state,
                        _ => unreachable!(),
                    };

                    let table = Arc::new(state.hash_table);

                    let result_types = self
                        .layout
                        .aggregates
                        .iter()
                        .map(|agg| agg.function.state.return_type.clone());

                    let scan_states = (0..self.partitions)
                        .map(|idx| {
                            let groups =
                                Batch::new(self.group_types_no_hash_iter(), self.batch_size)?;
                            let results = Batch::new(result_types.clone(), self.batch_size)?;

                            // Init this row scan state with a subset of blocks
                            // to scan.
                            let row_scan_state = RowScanState::new_partial_scan(
                                (0..table.data.num_row_blocks())
                                    .filter(|block_idx| block_idx % self.partitions == idx),
                            );

                            Ok(GroupingSetScanPartitionState {
                                hash_table: table.clone(),
                                scan_state: row_scan_state,
                                groups,
                                results,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    *op_state =
                        GroupingSetOperatorState::Scanning(HashTableScanningOperatorState {
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
        op_state: &mut GroupingSetOperatorState,
    ) -> Result<GroupingSetScanPartitionState> {
        match &mut *op_state {
            GroupingSetOperatorState::Scanning(scanning) => {
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
    pub fn scan(
        &self,
        state: &mut GroupingSetScanPartitionState,
        output: &mut Batch,
    ) -> Result<()> {
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
            output.set_constant_value(output_idx, BorrowedScalarValue::UInt64(grouping_val))?;
        }

        output.set_num_rows(group_row_count)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::PhysicalAggregateExpression;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::{self, FUNCTION_SET_SUM};
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
        let table = GroupingSetHashTable::new(&aggs, grouping_set, 16, 1);
        let (mut op_state, mut build_states) = table.init_states().unwrap();
        assert_eq!(1, build_states.len());

        let mut input = generate_batch!(["a", "b", "c", "a"], [1_i64, 2, 3, 4]);
        table.insert(&mut build_states[0], &mut input).unwrap();

        let scan_ready = table.merge(&mut op_state, &mut build_states[0]).unwrap();
        assert!(scan_ready);

        let mut scan_state = table.take_partition_scan_state(&mut op_state).unwrap();

        let mut out = Batch::new([DataType::Utf8, DataType::Int64], 16).unwrap();
        table.scan(&mut scan_state, &mut out).unwrap();

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
        let table = GroupingSetHashTable::new(&aggs, grouping_set, 16, 1);
        let (mut op_state, mut build_states) = table.init_states().unwrap();
        assert_eq!(1, build_states.len());

        let mut input = generate_batch!(
            ["a", "b", "c", "a"],
            [1_i64, 2, 3, 4],
            ["gg", "ff", "gg", "ff"]
        );
        table.insert(&mut build_states[0], &mut input).unwrap();

        let scan_ready = table.merge(&mut op_state, &mut build_states[0]).unwrap();
        assert!(scan_ready);

        let mut scan_state = table.take_partition_scan_state(&mut op_state).unwrap();

        let mut out = Batch::new([DataType::Utf8, DataType::Utf8, DataType::Int64], 16).unwrap();
        table.scan(&mut scan_state, &mut out).unwrap();

        let expected = generate_batch!([None as Option<&str>, None], ["gg", "ff"], [4_i64, 6]);
        assert_batches_eq(&expected, &out);
    }
}
