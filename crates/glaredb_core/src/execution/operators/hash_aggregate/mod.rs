pub mod distinct_aggregates;

mod aggregate_hash_table;
mod grouping_set_hash_table;
mod grouping_value;

use std::collections::BTreeSet;
use std::task::Context;

use distinct_aggregates::AggregateSelection;
use glaredb_error::{DbError, Result};
use grouping_set_hash_table::{
    GroupingSetHashTable,
    GroupingSetOperatorState,
    GroupingSetPartitionState,
    GroupingSetScanPartitionState,
};
use parking_lot::Mutex;

use super::util::partition_wakers::PartitionWakers;
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
    /// Partition state per grouping set table.
    states: Vec<GroupingSetPartitionState>,
}

#[derive(Debug)]
pub struct HashAggregateScanningPartitionState {
    partition_idx: usize,
    /// If the scan is ready.
    ///
    /// Stored on the partiton state to avoid needing to look at the operator
    /// state.
    ///
    /// If false, check operator state.
    scan_ready: bool,
    /// Scan states per grouping set tables.
    ///
    /// This should be treated as a queue, each scan state should be exhausted
    /// (and removed) until we have no scan states left.
    states: Vec<(usize, GroupingSetPartitionState)>,
}

#[derive(Debug)]
pub struct HashAggregateOperatorState {
    /// Hash table per grouping set.
    tables: Vec<GroupingSetHashTable>,
    /// State for each grouping set hash table.
    table_states: Vec<GroupingSetOperatorState>,
    inner: Mutex<HashAggregateOperatoreStateInner>,
}

#[derive(Debug)]
struct HashAggregateOperatoreStateInner {
    /// Indicator for if each table is ready for scanning.
    scan_ready: Vec<bool>,
    /// Wakers for partitions that finished their inputs and are waiting for the
    /// global table to be ready.
    wakers: PartitionWakers,
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
    /// Distinct/non-distinct aggregate indices.
    pub(crate) agg_selection: AggregateSelection,
}

impl PhysicalHashAggregate {
    pub fn new(aggregates: Aggregates, grouping_sets: Vec<BTreeSet<usize>>) -> Self {
        let mut output_types = Vec::new();

        let agg_selection = AggregateSelection::new(&aggregates.aggregates);

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
            agg_selection,
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
            .map(|grouping_set| GroupingSetHashTable::new(&self.aggregates, grouping_set.clone()))
            .collect();

        let table_states = tables
            .iter()
            .map(|table| table.create_operator_state(props.batch_size))
            .collect::<Result<Vec<_>>>()?;

        let inner = HashAggregateOperatoreStateInner {
            scan_ready: vec![false; tables.len()],
            wakers: PartitionWakers::empty(), // Set to correct size when we get the partition states.
        };

        Ok(HashAggregateOperatorState {
            tables,
            table_states,
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
        inner.wakers.init_for_partitions(partitions);

        debug_assert_eq!(
            operator_state.table_states.len(),
            operator_state.tables.len()
        );

        // Generate build states for each partition.
        for (table, op_state) in operator_state
            .tables
            .iter()
            .zip(&operator_state.table_states)
        {
            let table_partition_states = table.create_partition_states(op_state, partitions)?;

            debug_assert_eq!(partition_states.len(), table_partition_states.len());
            for (partition_state, table_state) in
                partition_states.iter_mut().zip(table_partition_states)
            {
                partition_state.states.push(table_state);
            }
        }

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

                // TODO: Distinct updates.

                // Insert input into each grouping set table.
                for (table, state) in operator_state.tables.iter().zip(&mut building.states) {
                    table.insert(state, &self.agg_selection.non_distinct, input)?;
                }

                Ok(PollExecute::NeedsMore)
            }
            HashAggregatePartitionState::Scanning(scanning) => {
                if !scanning.scan_ready {
                    // Check operator state to really see if the scan is ready
                    // or not.
                    let mut shared_state = operator_state.inner.lock();
                    let scan_ready = shared_state.scan_ready.iter().all(|&ready| ready);
                    if !scan_ready {
                        // Come back later.
                        shared_state
                            .wakers
                            .store(cx.waker(), scanning.partition_idx);
                        return Ok(PollExecute::Pending);
                    }

                    // We're good to scan, continue on...
                    scanning.scan_ready = true;
                }

                loop {
                    // Fill up our output batch by trying to scan as many tables
                    // as we can.
                    let (table_idx, mut scan_state) = match scanning.states.pop() {
                        Some(v) => v,
                        None => {
                            // No more states, we're completely exhausted.
                            output.set_num_rows(0)?;
                            return Ok(PollExecute::Exhausted);
                        }
                    };

                    let table = &operator_state.tables[table_idx];
                    let table_op_state = &operator_state.table_states[table_idx];

                    table.scan(table_op_state, &mut scan_state, output)?;
                    if output.num_rows == 0 {
                        // Scan produced no rows, try the next state.
                        continue;
                    }

                    // Scan produced rows, push this state back onto the queue
                    // for the next poll.
                    scanning.states.push((table_idx, scan_state));

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

                // Note we track scan readiness per table since we may be
                // finalizing multiple partitions at once, and the order in
                // which we acquire the locks in the table isn't guaranteed.
                //
                // We only update the operator state for tables that return
                // 'true' from the merge, indicating that table is ready.
                //
                // Then every partition will check if all tables are set to
                // 'true', and if so, will wake everyone up.
                let mut ready = vec![false; operator_state.tables.len()];

                for table_idx in 0..operator_state.tables.len() {
                    let scan_ready = operator_state.tables[table_idx].merge(
                        &operator_state.table_states[table_idx],
                        &mut building.states[table_idx],
                        self.agg_selection.non_distinct.iter().copied(),
                    )?;

                    ready[table_idx] = scan_ready;
                }

                // Attach table indices to the states. We're going to drain the
                // states as a queue during draining, so we need to preserve the
                // table index the state is for.
                let table_states: Vec<_> = building.states.drain(..).enumerate().collect();
                *state =
                    HashAggregatePartitionState::Scanning(HashAggregateScanningPartitionState {
                        partition_idx: building.partition_idx,
                        scan_ready: false,
                        states: table_states,
                    });

                let mut shared_state = operator_state.inner.lock();
                for (table_idx, ready) in ready.into_iter().enumerate() {
                    if ready {
                        shared_state.scan_ready[table_idx] = true;
                    }
                }

                let scan_ready = shared_state.scan_ready.iter().all(|&ready| ready);
                if scan_ready {
                    // Wake up all partitions, we're ready to produce results.
                    shared_state.wakers.wake_all();
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
