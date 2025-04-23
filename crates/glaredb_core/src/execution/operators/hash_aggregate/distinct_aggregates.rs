use std::collections::BTreeSet;

use glaredb_error::Result;

use super::grouping_set_hash_table::{
    GroupingSetBuildPartitionState,
    GroupingSetHashTable,
    GroupingSetOperatorState,
};
use crate::arrays::batch::Batch;
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
    /// Input arguments to the aggregate that should be distinct.
    pub inputs: &'a [PhysicalColumnExpr],
    /// Group keys we'll be DISTINCTing on as well.
    pub groups: &'a [PhysicalColumnExpr],
}

#[derive(Debug)]
pub struct DistinctCollectionOperatorState {
    /// Operator states for all distinct tables.
    states: Vec<GroupingSetOperatorState>,
}

#[derive(Debug)]
pub struct DistinctCollectionPartitionState {
    /// Build state per distinct table.
    states: Vec<GroupingSetBuildPartitionState>,
}

#[derive(Debug)]
struct DistinctTable {
    /// The table holding the inputs.
    table: GroupingSetHashTable,
    /// Inputs we're distincting on.
    inputs: Vec<PhysicalColumnExpr>,
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
    /// Mapping of DISTINCT aggregates to table to insert into.
    agg_to_table_index: Vec<usize>,
}

impl DistinctCollection {
    pub fn new<'a>(
        aggregates: impl IntoIterator<Item = DistinctAggregateInfo<'a>>,
    ) -> Result<Self> {
        let mut tables: Vec<DistinctTable> = Vec::new();
        let mut agg_to_table_index = Vec::new();

        for agg in aggregates {
            // We're going to be DISTINCTing on both the aggregate inputs and
            // the groups.
            let inputs: Vec<_> = agg
                .inputs
                .iter()
                .cloned()
                .chain(agg.groups.iter().cloned())
                .collect();

            // Try to find an exisitng table with the same set of inputs.
            let table_idx = match tables.iter().position(|table| table.inputs_match(&inputs)) {
                Some(table_idx) => table_idx,
                None => {
                    // Create a new table for these inputs.
                    let aggregates = Aggregates {
                        groups: inputs.clone(),
                        grouping_functions: Vec::new(),
                        aggregates: Vec::new(),
                    };
                    let grouping_set: BTreeSet<_> = (0..inputs.len()).collect();
                    let table = GroupingSetHashTable::new(&aggregates, grouping_set);

                    let table_idx = tables.len();
                    tables.push(DistinctTable { table, inputs });

                    table_idx
                }
            };

            agg_to_table_index.push(table_idx);
        }

        Ok(DistinctCollection {
            tables,
            agg_to_table_index,
        })
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
        op_state: &mut DistinctCollectionOperatorState,
        partitions: usize,
    ) -> Result<Vec<DistinctCollectionPartitionState>> {
        debug_assert_eq!(op_state.states.len(), self.tables.len());

        let mut part_states: Vec<_> = (0..partitions)
            .map(|_| DistinctCollectionPartitionState {
                states: Vec::with_capacity(self.tables.len()),
            })
            .collect();

        for (table, op_state) in self.tables.iter().zip(&mut op_state.states) {
            let states = table
                .table
                .create_partition_build_states(op_state, partitions)?;

            for (out, state) in part_states.iter_mut().zip(states) {
                out.states.push(state);
            }
        }

        Ok(part_states)
    }

    pub fn insert(
        &self,
        _op_state: &DistinctCollectionOperatorState,
        state: &mut DistinctCollectionPartitionState,
        input: &mut Batch,
    ) -> Result<()> {
        debug_assert_eq!(self.tables.len(), state.states.len());

        for (table, state) in self.tables.iter().zip(&mut state.states) {
            // No agg selection since we don't have any aggs in the hash table.
            // It's just a big GROUP BY.
            table.table.insert(state, [], input)?;
        }

        Ok(())
    }

    pub fn merge(
        &self,
        op_state: &mut DistinctCollectionOperatorState,
        state: &mut DistinctCollectionPartitionState,
    ) -> Result<()> {
        debug_assert_eq!(self.tables.len(), op_state.states.len());
        debug_assert_eq!(self.tables.len(), state.states.len());

        let state_iter = op_state.states.iter_mut().zip(&mut state.states);

        for (table, (op_state, part_state)) in self.tables.iter().zip(state_iter) {
            let _ = table.table.merge(op_state, part_state)?;
        }

        Ok(())
    }
}
