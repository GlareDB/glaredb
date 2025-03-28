pub mod distinct_aggregates;

mod aggregate_hash_table;
mod grouping_set_hash_table;
mod grouping_value;

use std::collections::BTreeSet;
use std::task::{Context, Waker};

use glaredb_error::{DbError, Result};
use grouping_set_hash_table::{
    GroupingSetBuildPartitionState,
    GroupingSetHashTable,
    GroupingSetOperatorState,
    GroupingSetScanPartitionState,
};
use parking_lot::Mutex;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalAggregateExpression;
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::logical::logical_aggregate::GroupingFunction;

#[derive(Debug)]
pub struct Aggregates {
    /// Columns making up the groups.
    pub groups: Vec<PhysicalColumnExpr>,
    /// GROUPING functions.
    pub grouping_functions: Vec<GroupingFunction>,
    /// Aggregate expressions.
    pub aggregates: Vec<PhysicalAggregateExpression>,
}

#[derive(Debug)]
pub enum HashAggregatePartitionState {
    Building(HashAggregateBuildingPartitionState),
    Scanning(HashAggregateScanningPartitionState),
}

#[derive(Debug)]
pub struct HashAggregateBuildingPartitionState {
    partition_idx: usize,
    /// Build states per grouping set table.
    states: Vec<GroupingSetBuildPartitionState>,
}

#[derive(Debug)]
pub struct HashAggregateScanningPartitionState {
    partition_idx: usize,
    /// If we need to populate the scan states.
    needs_populate: bool,
    /// Scan states per grouping set tables.
    ///
    /// This should be treated as a queue, each scan state should be exhausted
    /// (and removed) until we have no scan states left.
    states: Vec<(GroupingSetScanPartitionState, usize)>,
}

#[derive(Debug)]
pub struct HashAggregateOperatorState {
    /// Hash table per grouping set.
    tables: Vec<GroupingSetHashTable>,
    inner: Mutex<HashAggregateOperatoreStateInner>,
}

#[derive(Debug)]
struct HashAggregateOperatoreStateInner {
    /// If all tables are ready for scanning.
    scan_ready: bool,
    /// State for each grouping set hash table.
    table_states: Vec<GroupingSetOperatorState>,
    /// Wakers for partitions that finished their inputs and are waiting for the
    /// global table to be ready.
    wakers: Vec<Option<Waker>>,
}

/// Compute aggregates over input batches.
///
/// Output batch layout: [GROUP_VALS, AGG_RESULTS, GROUPING_VALS]
#[derive(Debug)]
pub struct PhysicalHashAggregate {
    /// All grouping sets we're grouping by.
    pub(crate) grouping_sets: Vec<BTreeSet<usize>>,
    /// Aggregates we're working on.
    pub(crate) aggregates: Aggregates,
    pub(crate) output_types: Vec<DataType>,
}

impl PhysicalHashAggregate {
    pub fn new(aggregates: Aggregates, grouping_sets: Vec<BTreeSet<usize>>) -> Self {
        let mut output_types = Vec::new();

        for group in &aggregates.groups {
            output_types.push(group.datatype.clone());
        }
        for agg in &aggregates.aggregates {
            output_types.push(agg.function.state.return_type.clone());
        }
        for _ in 0..aggregates.grouping_functions.len() {
            output_types.push(DataType::Int64);
        }

        PhysicalHashAggregate {
            grouping_sets,
            aggregates,
            output_types,
        }
    }
}

impl BaseOperator for PhysicalHashAggregate {
    const OPERATOR_NAME: &str = "HashAggregate";

    type OperatorState = HashAggregateOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        // Table per grouping set.
        let tables: Vec<_> = self
            .grouping_sets
            .iter()
            .map(|grouping_set| {
                GroupingSetHashTable::new(&self.aggregates, grouping_set.clone(), props.batch_size)
            })
            .collect();

        let inner = HashAggregateOperatoreStateInner {
            scan_ready: false,
            table_states: Vec::with_capacity(tables.len()), // Populated below when we get the partition states.
            wakers: Vec::new(), // Set to correct size when we get the partition states.
        };

        Ok(HashAggregateOperatorState {
            tables,
            inner: Mutex::new(inner),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl ExecuteOperator for PhysicalHashAggregate {
    type PartitionExecuteState = HashAggregatePartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let mut partition_states: Vec<_> = (0..partitions)
            .map(|idx| {
                HashAggregateBuildingPartitionState {
                    partition_idx: idx,
                    states: Vec::with_capacity(operator_state.tables.len()), // Populated below
                }
            })
            .collect();

        let inner = &mut operator_state.inner.lock();
        for table in &operator_state.tables {
            let (table_op_state, table_partition_states) = table.init_states(partitions)?;
            inner.table_states.push(table_op_state);

            debug_assert_eq!(partition_states.len(), table_partition_states.len());
            for (partition_state, table_state) in
                partition_states.iter_mut().zip(table_partition_states)
            {
                partition_state.states.push(table_state);
            }
        }

        // Resize partition waker vec.
        inner.wakers.resize(partitions, None);

        let partition_states = partition_states
            .into_iter()
            .map(HashAggregatePartitionState::Building)
            .collect();

        Ok(partition_states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        match state {
            HashAggregatePartitionState::Building(building) => {
                debug_assert_eq!(building.states.len(), operator_state.tables.len());

                // Insert input into each grouping set table.
                for (table, state) in operator_state.tables.iter().zip(&mut building.states) {
                    table.insert(state, input)?;
                }

                Ok(PollExecute::NeedsMore)
            }
            HashAggregatePartitionState::Scanning(scanning) => {
                if scanning.needs_populate {
                    // Get states from operator state.
                    let mut shared_state = operator_state.inner.lock();
                    if !shared_state.scan_ready {
                        // Come back later.
                        shared_state.wakers[scanning.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollExecute::Pending);
                    }

                    // All tables ready, take a scan state for each.
                    debug_assert_eq!(operator_state.tables.len(), shared_state.table_states.len());
                    for (idx, (table, table_state)) in operator_state
                        .tables
                        .iter()
                        .zip(&mut shared_state.table_states)
                        .enumerate()
                    {
                        let scan_state = table.take_partition_scan_state(table_state)?;
                        scanning.states.push((scan_state, idx));
                    }

                    scanning.needs_populate = false;

                    // Continue on, we have all the scan states we need.
                }

                loop {
                    // Fill up our output batch by trying to scan as many tables
                    // as we can.
                    let (mut scan_state, table_idx) = match scanning.states.pop() {
                        Some(v) => v,
                        None => {
                            // No more states, we're completely exhausted.
                            output.set_num_rows(0)?;
                            return Ok(PollExecute::Exhausted);
                        }
                    };

                    let table = &operator_state.tables[table_idx];

                    table.scan(&mut scan_state, output)?;
                    if output.num_rows == 0 {
                        // Scan produced no rows, try the next state.
                        continue;
                    }

                    // Scan produced rows, push this state back onto the queue
                    // for the next poll.
                    scanning.states.push((scan_state, table_idx));

                    return Ok(PollExecute::HasMore);
                }
            }
        }
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        match state {
            HashAggregatePartitionState::Building(building) => {
                // Finalize the building for this partition by merging all
                // partition-local tables into the operator tables.

                let partition_idx = building.partition_idx;
                // Replace partition state to ready it for scanning.
                let state = std::mem::replace(
                    state,
                    HashAggregatePartitionState::Scanning(HashAggregateScanningPartitionState {
                        partition_idx,
                        needs_populate: true,
                        states: Vec::with_capacity(self.grouping_sets.len()),
                    }),
                );

                // Get the partition-local hash tables, merge with operator hash
                // tables.
                let mut partition_state = match state {
                    HashAggregatePartitionState::Building(state) => state,
                    HashAggregatePartitionState::Scanning(_) => unreachable!(),
                };

                let mut shared_state = operator_state.inner.lock();

                for table_idx in 0..operator_state.tables.len() {
                    let scan_ready = operator_state.tables[table_idx].merge(
                        &mut shared_state.table_states[table_idx],
                        &mut partition_state.states[table_idx],
                    )?;

                    // Just set if we're ready on the first table. If the first
                    // table is ready for scanning, then all tables should be
                    // ready for scanning.
                    if table_idx == 0 {
                        shared_state.scan_ready = scan_ready
                    }
                    debug_assert_eq!(shared_state.scan_ready, scan_ready);
                }

                if shared_state.scan_ready {
                    // Wake up all partitions, we're ready to produce results.
                    for waker in &mut shared_state.wakers {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                    }
                }

                Ok(PollFinalize::NeedsDrain)
            }
            _ => Err(DbError::new("Hash aggregate partition already finalized")),
        }
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::expr::{self, bind_aggregate_function};
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn hash_aggregate_single_partition() {
        // SUM(col0) GROUP BY (col1)

        // AGG_INPUT (col0): Int64
        // GROUP     (col1): Utf8
        let sum_agg = bind_aggregate_function(
            &FUNCTION_SET_SUM,
            vec![expr::column((0, 0), DataType::Int64).into()],
        )
        .unwrap();

        let aggs = Aggregates {
            groups: vec![(1, DataType::Utf8).into()],
            grouping_functions: Vec::new(),
            aggregates: vec![PhysicalAggregateExpression::new(
                sum_agg,
                [(0, DataType::Int64)],
            )],
        };

        let wrapper = OperatorWrapper::new(PhysicalHashAggregate::new(
            aggs,
            vec![[0].into_iter().collect()],
        ));

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut output = Batch::new(wrapper.operator.output_types.clone(), 16).unwrap();
        let mut input = generate_batch!(
            [1_i64, 2, 3, 4],
            ["group_a", "group_b", "group_a", "group_c"],
        );

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut states[0])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!(["group_a", "group_b", "group_c",], [4_i64, 2, 4],);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);
        assert_eq!(0, output.num_rows);
    }
}
