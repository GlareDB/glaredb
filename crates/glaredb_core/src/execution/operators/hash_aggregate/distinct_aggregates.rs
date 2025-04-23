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

#[expect(unused)]
#[derive(Debug)]
pub struct DistinctAggregateOperatorState {
    /// Operator states for each hash table.
    operator_states: Vec<GroupingSetOperatorState>,
}

#[expect(unused)]
#[derive(Debug)]
pub struct DistinctAggregatePartitionState {
    /// Partition state per table.
    partition_states: Vec<GroupingSetBuildPartitionState>,
    /// Output buffers for each table once we've pushed all data into them.
    output_buffers: Batch,
}

#[derive(Debug, Clone)]
pub struct DistinctAggregateInfo {
    /// Input arguments to the aggregate that should be distinct.
    pub inputs: Vec<PhysicalColumnExpr>,
    /// Group keys we'll be DISTINCTing on as well.
    pub groups: Vec<PhysicalColumnExpr>,
}

#[expect(unused)]
#[derive(Debug)]
pub struct DistinctAggregateInputs {
    /// Hash tables holding the inputs to distinct aggregates.
    tables: Vec<GroupingSetHashTable>,
    /// Info for each distinct aggregate input.
    infos: Vec<DistinctAggregateInfo>,
}

impl DistinctAggregateInputs {
    pub fn new(
        infos: impl IntoIterator<Item = DistinctAggregateInfo>,
        batch_size: usize,
    ) -> Result<Self> {
        let infos: Vec<_> = infos.into_iter().collect();
        let mut tables = Vec::with_capacity(infos.len());

        for info in &infos {
            // Create an aggregates object that just groups on all
            // inputs/groups.
            let mut groups = info.inputs.clone();
            groups.extend_from_slice(&info.groups);

            let aggregates = Aggregates {
                groups,
                grouping_functions: Vec::new(),
                aggregates: Vec::new(),
            };

            let grouping_set = (0..aggregates.groups.len()).collect();

            let table = GroupingSetHashTable::new(&aggregates, grouping_set, batch_size);
            tables.push(table);
        }

        Ok(DistinctAggregateInputs { tables, infos })
    }
}
