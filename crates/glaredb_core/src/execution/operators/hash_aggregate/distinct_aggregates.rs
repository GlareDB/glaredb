use std::collections::BTreeSet;

use glaredb_error::Result;

use super::grouping_set_hash_table::{
    GroupingSetHashTable,
    GroupingSetOperatorState,
    GroupingSetPartitionState,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::execution::operators::hash_aggregate::Aggregates;
use crate::expr::physical::PhysicalAggregateExpression;
use crate::expr::physical::column_expr::PhysicalColumnExpr;

/// Aggregate selections for determining which aggregates are or are not
/// distinct.
#[derive(Debug)]
pub struct AggregateSelection {
    pub distinct: Vec<usize>,
    pub non_distinct: Vec<usize>,
}

impl AggregateSelection {
    pub fn new<'a>(aggs: impl IntoIterator<Item = &'a PhysicalAggregateExpression>) -> Self {
        let mut distinct = Vec::new();
        let mut non_distinct = Vec::new();

        for (idx, agg) in aggs.into_iter().enumerate() {
            if agg.is_distinct {
                distinct.push(idx);
            } else {
                non_distinct.push(idx);
            }
        }

        AggregateSelection {
            distinct,
            non_distinct,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DistinctAggregateInfo<'a> {
    /// Group keys we'll be DISTINCTing on as well.
    pub groups: &'a [PhysicalColumnExpr],
    /// Input arguments to the aggregate that should be distinct.
    pub inputs: &'a [PhysicalColumnExpr],
}

#[derive(Debug)]
pub struct DistinctCollectionOperatorState {
    /// Operator states for all distinct tables.
    states: Vec<GroupingSetOperatorState>,
}

#[derive(Debug)]
pub struct DistinctCollectionPartitionState {
    /// Partition state per distinct table.
    states: Vec<GroupingSetPartitionState>,
}

#[derive(Debug)]
struct DistinctTable {
    /// The table holding the inputs.
    table: GroupingSetHashTable,
    /// Inputs we're distincting on.
    inputs: Vec<PhysicalColumnExpr>,
    /// Aggregate indices that this table corresponds to.
    table_to_agg_index: Vec<usize>,
}

impl DistinctTable {
    /// If this set of distinct inputs matches the other set of inputs.
    ///
    /// Used to determine if the same hash table can be used for multiple
    /// DISTINCT aggregates.
    fn inputs_match(&self, other: &[PhysicalColumnExpr]) -> bool {
        if self.inputs.len() != other.len() {
            return false;
        }

        for (this, other) in self.inputs.iter().zip(other) {
            if this.idx != other.idx {
                return false;
            }
        }

        true
    }
}

#[derive(Debug)]
pub struct DistinctCollection {
    /// Hash tables holding the inputs to distinct aggregates.
    tables: Vec<DistinctTable>,
}

impl DistinctCollection {
    pub fn new<'a>(
        aggregates: impl IntoIterator<Item = DistinctAggregateInfo<'a>>,
    ) -> Result<Self> {
        let mut tables: Vec<DistinctTable> = Vec::new();

        for (agg_idx, agg) in aggregates.into_iter().enumerate() {
            // We're going to be DISTINCTing on both the aggregate inputs and
            // the groups.
            let inputs: Vec<_> = agg
                .groups
                .iter()
                .cloned()
                .chain(agg.inputs.iter().cloned())
                .collect();

            // Try to find an exisitng table with the same set of inputs.
            match tables.iter_mut().find(|table| table.inputs_match(&inputs)) {
                Some(table) => {
                    table.table_to_agg_index.push(agg_idx);
                }
                None => {
                    // Create a new table for these inputs.
                    let aggregates = Aggregates {
                        groups: inputs.clone(),
                        grouping_functions: Vec::new(),
                        aggregates: Vec::new(),
                    };
                    let grouping_set: BTreeSet<_> = (0..inputs.len()).collect();
                    let table = GroupingSetHashTable::new(&aggregates, grouping_set);

                    tables.push(DistinctTable {
                        table,
                        inputs,
                        table_to_agg_index: vec![agg_idx],
                    });
                }
            };
        }

        Ok(DistinctCollection { tables })
    }

    /// Number of tables that are DISTINCTing aggregate inputs.
    ///
    /// May be less than the number of distinct aggregates if multiple
    /// aggregates share the same input.
    pub fn num_distinct_tables(&self) -> usize {
        self.tables.len()
    }

    /// Return an iterator of datatypes in a given table.
    ///
    /// The return types are ordered GROUP types first, then input types.
    pub fn iter_table_types(&self, table_idx: usize) -> impl ExactSizeIterator<Item = DataType> {
        self.tables[table_idx]
            .inputs
            .iter()
            .map(|expr| expr.datatype.clone())
    }

    /// Returns aggregate indices that a table is producing unique inputs for.
    pub fn aggregates_for_table(&self, table_idx: usize) -> &[usize] {
        &self.tables[table_idx].table_to_agg_index
    }

    pub fn create_operator_state(
        &self,
        batch_size: usize,
    ) -> Result<DistinctCollectionOperatorState> {
        let states = self
            .tables
            .iter()
            .map(|table| table.table.create_operator_state(batch_size))
            .collect::<Result<Vec<_>>>()?;

        Ok(DistinctCollectionOperatorState { states })
    }

    pub fn create_partition_states(
        &self,
        op_state: &DistinctCollectionOperatorState,
        partitions: usize,
    ) -> Result<Vec<DistinctCollectionPartitionState>> {
        debug_assert_eq!(op_state.states.len(), self.tables.len());

        let mut part_states: Vec<_> = (0..partitions)
            .map(|_| DistinctCollectionPartitionState {
                states: Vec::with_capacity(self.tables.len()),
            })
            .collect();

        for (table, op_state) in self.tables.iter().zip(&op_state.states) {
            let states = table.table.create_partition_states(op_state, partitions)?;

            for (out, state) in part_states.iter_mut().zip(states) {
                out.states.push(state);
            }
        }

        Ok(part_states)
    }

    pub fn insert(
        &self,
        state: &mut DistinctCollectionPartitionState,
        input: &mut Batch,
    ) -> Result<()> {
        debug_assert_eq!(self.tables.len(), state.states.len());

        for (table, state) in self.tables.iter().zip(&mut state.states) {
            // No agg selection since we don't have any aggs in the hash table.
            // It's just a big GROUP BY.
            table.table.insert_input(state, &[], input)?;
        }

        Ok(())
    }

    /// Merge the local table into the global table.
    pub fn merge(
        &self,
        op_state: &DistinctCollectionOperatorState,
        state: &mut DistinctCollectionPartitionState,
    ) -> Result<()> {
        debug_assert_eq!(self.tables.len(), op_state.states.len());
        debug_assert_eq!(self.tables.len(), state.states.len());

        let state_iter = op_state.states.iter().zip(&mut state.states);

        for (table, (op_state, part_state)) in self.tables.iter().zip(state_iter) {
            let _ = table.table.merge(op_state, part_state)?;
        }

        Ok(())
    }

    /// Scan the distinct table.
    ///
    /// This may be called concurrently with other partitions, each partition
    /// will be readining a disjoint set of rows.
    pub fn scan(
        &self,
        op_state: &DistinctCollectionOperatorState,
        state: &mut DistinctCollectionPartitionState,
        table_idx: usize,
        output: &mut Batch,
    ) -> Result<()> {
        let op_state = &op_state.states[table_idx];
        let state = &mut state.states[table_idx];

        self.tables[table_idx].table.scan(op_state, state, output)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn distinct_single_agg_single_column_input() {
        // AGG 1: INPUT [0], GROUP []
        let collection = DistinctCollection::new([DistinctAggregateInfo {
            inputs: &[(0, DataType::Int32).into()],
            groups: &[],
        }])
        .unwrap();
        assert_eq!(1, collection.num_distinct_tables());
        assert_eq!(&[0], collection.aggregates_for_table(0));

        let op_state = collection.create_operator_state(16).unwrap();
        let mut part_states = collection.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut b = generate_batch!([1, 2, 3, 3, 4], ["a", "b", "c", "d", "e"]);
        collection.insert(&mut part_states[0], &mut b).unwrap();
        collection.merge(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::Int32], 16).unwrap();
        collection
            .scan(&op_state, &mut part_states[0], 0, &mut out)
            .unwrap();

        let expected = generate_batch!([1, 2, 3, 4]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn distinct_single_agg_single_column_input_not_first() {
        // AGG 1: INPUT [1], GROUP []
        let collection = DistinctCollection::new([DistinctAggregateInfo {
            inputs: &[(1, DataType::Utf8).into()],
            groups: &[],
        }])
        .unwrap();
        assert_eq!(1, collection.num_distinct_tables());
        assert_eq!(&[0], collection.aggregates_for_table(0));

        let op_state = collection.create_operator_state(16).unwrap();
        let mut part_states = collection.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut b = generate_batch!([1, 2, 3, 3, 4], ["a", "b", "b", "a", "a"]);
        collection.insert(&mut part_states[0], &mut b).unwrap();
        collection.merge(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::Utf8], 16).unwrap();
        collection
            .scan(&op_state, &mut part_states[0], 0, &mut out)
            .unwrap();

        let expected = generate_batch!(["a", "b"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn distinct_single_agg_two_column_input() {
        // AGG 1: INPUT [0, 1], GROUP []
        let collection = DistinctCollection::new([DistinctAggregateInfo {
            inputs: &[(0, DataType::Int32).into(), (1, DataType::Utf8).into()],
            groups: &[],
        }])
        .unwrap();
        assert_eq!(1, collection.num_distinct_tables());
        assert_eq!(&[0], collection.aggregates_for_table(0));

        let op_state = collection.create_operator_state(16).unwrap();
        let mut part_states = collection.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut b = generate_batch!([1, 3, 3, 3, 1], ["a", "b", "b", "a", "a"]);
        collection.insert(&mut part_states[0], &mut b).unwrap();
        collection.merge(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::Int32, DataType::Utf8], 16).unwrap();
        collection
            .scan(&op_state, &mut part_states[0], 0, &mut out)
            .unwrap();

        let expected = generate_batch!([1, 3, 3], ["a", "b", "a"]);
        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn distinct_two_aggs_different_inputs() {
        // AGG 1: INPUT [0], GROUP []
        // AGG 2: INPUT [1], GROUP []
        let collection = DistinctCollection::new([
            DistinctAggregateInfo {
                inputs: &[(0, DataType::Int32).into()],
                groups: &[],
            },
            DistinctAggregateInfo {
                inputs: &[(1, DataType::Utf8).into()],
                groups: &[],
            },
        ])
        .unwrap();
        assert_eq!(2, collection.num_distinct_tables());
        assert_eq!(&[0], collection.aggregates_for_table(0));
        assert_eq!(&[1], collection.aggregates_for_table(1));

        let op_state = collection.create_operator_state(16).unwrap();
        let mut part_states = collection.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut b = generate_batch!([1, 3, 3, 3, 1], ["a", "b", "b", "a", "c"]);
        collection.insert(&mut part_states[0], &mut b).unwrap();
        collection.merge(&op_state, &mut part_states[0]).unwrap();

        let mut out_agg1 = Batch::new([DataType::Int32], 16).unwrap();
        collection
            .scan(&op_state, &mut part_states[0], 0, &mut out_agg1)
            .unwrap();

        let expected = generate_batch!([1, 3]);
        assert_batches_eq(&expected, &out_agg1);

        let mut out_agg2 = Batch::new([DataType::Utf8], 16).unwrap();
        collection
            .scan(&op_state, &mut part_states[0], 1, &mut out_agg2)
            .unwrap();

        let expected = generate_batch!(["a", "b", "c"]);
        assert_batches_eq(&expected, &out_agg2);
    }

    #[test]
    fn distinct_two_aggs_shared_inputs() {
        // AGG 1: INPUT [0, 1], GROUP []
        // AGG 2: INPUT [0, 1], GROUP []
        let collection = DistinctCollection::new([
            DistinctAggregateInfo {
                inputs: &[(0, DataType::Int32).into(), (1, DataType::Utf8).into()],
                groups: &[],
            },
            DistinctAggregateInfo {
                inputs: &[(0, DataType::Int32).into(), (1, DataType::Utf8).into()],
                groups: &[],
            },
        ])
        .unwrap();
        assert_eq!(1, collection.num_distinct_tables());
        assert_eq!(&[0, 1], collection.aggregates_for_table(0));

        let op_state = collection.create_operator_state(16).unwrap();
        let mut part_states = collection.create_partition_states(&op_state, 1).unwrap();
        assert_eq!(1, part_states.len());

        let mut b = generate_batch!([1, 3, 3, 3, 1], ["a", "b", "b", "a", "c"]);
        collection.insert(&mut part_states[0], &mut b).unwrap();
        collection.merge(&op_state, &mut part_states[0]).unwrap();

        let mut out = Batch::new([DataType::Int32, DataType::Utf8], 16).unwrap();
        collection
            .scan(&op_state, &mut part_states[0], 0, &mut out)
            .unwrap();

        let expected = generate_batch!([1, 3, 3, 1], ["a", "b", "a", "c"]);
        assert_batches_eq(&expected, &out);
    }
}
