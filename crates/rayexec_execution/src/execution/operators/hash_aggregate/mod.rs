mod aggregate_hash_table;
mod grouping_set_hash_table;
mod grouping_value;

use std::collections::BTreeSet;
use std::task::{Context, Waker};

use grouping_set_hash_table::{
    GroupingSetBuildPartitionState,
    GroupingSetHashTable,
    GroupingSetOperatorState,
    GroupingSetScanPartitionState,
};
use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{ExecuteInOutState, PollExecute, PollFinalize, UnaryInputStates};
use crate::database::DatabaseContext;
use crate::execution::operators::{ExecutableOperator, OperatorState, PartitionState};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::expr::physical::PhysicalAggregateExpression;
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
    /// (and remove) until we have no scan states left.
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
}

impl ExecutableOperator for PhysicalHashAggregate {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        // Table per grouping set.
        let tables: Vec<_> = self
            .grouping_sets
            .iter()
            .map(|grouping_set| {
                GroupingSetHashTable::new(
                    &self.aggregates,
                    grouping_set.clone(),
                    batch_size,
                    partitions,
                )
            })
            .collect();

        let mut partition_states: Vec<_> = (0..partitions)
            .map(|idx| {
                HashAggregateBuildingPartitionState {
                    partition_idx: idx,
                    states: Vec::with_capacity(tables.len()), // Populated below
                }
            })
            .collect();

        let mut inner = HashAggregateOperatoreStateInner {
            scan_ready: false,
            table_states: Vec::with_capacity(tables.len()), // Populated below.
            wakers: (0..partitions).map(|_| None).collect(),
        };

        for table in &tables {
            let (table_op_state, table_partition_states) = table.init_states()?;
            inner.table_states.push(table_op_state);

            debug_assert_eq!(partition_states.len(), table_partition_states.len());

            for (partition_state, table_state) in
                partition_states.iter_mut().zip(table_partition_states)
            {
                partition_state.states.push(table_state);
            }
        }

        let partition_states = partition_states
            .into_iter()
            .map(|state| {
                PartitionState::HashAggregate(HashAggregatePartitionState::Building(state))
            })
            .collect();

        let operator_state = OperatorState::HashAggregate(HashAggregateOperatorState {
            tables,
            inner: Mutex::new(inner),
        });

        Ok(UnaryInputStates {
            operator_state,
            partition_states,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let op_state = match operator_state {
            OperatorState::HashAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Building(building) => {
                let input = inout.input.required("input batch required")?;

                debug_assert_eq!(building.states.len(), op_state.tables.len());

                // Insert input into each grouping set table.
                for (table, state) in op_state.tables.iter().zip(&mut building.states) {
                    table.insert(state, input)?;
                }

                Ok(PollExecute::NeedsMore)
            }
            HashAggregatePartitionState::Scanning(scanning) => {
                let output = inout.output.required("output batch required")?;

                if scanning.needs_populate {
                    // Get states from operator state.
                    let mut shared_state = op_state.inner.lock();
                    if !shared_state.scan_ready {
                        // Come back later.
                        shared_state.wakers[scanning.partition_idx] = Some(cx.waker().clone());
                        return Ok(PollExecute::Pending);
                    }

                    // All tables ready, take a scan state for each.
                    debug_assert_eq!(op_state.tables.len(), shared_state.table_states.len());
                    for (idx, (table, table_state)) in op_state
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
                    let (mut scan_state, table_idx) = match scanning.states.pop() {
                        Some(v) => v,
                        None => {
                            // No more states, we're completely exhausted.
                            output.set_num_rows(0)?;
                            return Ok(PollExecute::Exhausted);
                        }
                    };

                    let table = &op_state.tables[table_idx];

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

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::HashAggregate(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let op_state = match operator_state {
            OperatorState::HashAggregate(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        match state {
            HashAggregatePartitionState::Building(building) => {
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

                let mut shared_state = op_state.inner.lock();

                for table_idx in 0..op_state.tables.len() {
                    let scan_ready = op_state.tables[table_idx].merge(
                        &mut shared_state.table_states[table_idx],
                        &mut partition_state.states[table_idx],
                    )?;

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
            _ => Err(RayexecError::new(
                "Hash aggregate partition already finalized",
            )),
        }
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashAggregate")
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

        let mut operator = OperatorWrapper::new(PhysicalHashAggregate {
            grouping_sets: vec![[0].into_iter().collect()],
            aggregates: aggs,
        });

        let mut states = operator.create_unary_states(16, 1);

        let mut output = Batch::new([DataType::Utf8, DataType::Int64], 16).unwrap();
        let mut input = generate_batch!(
            [1_i64, 2, 3, 4],
            ["group_a", "group_b", "group_a", "group_c"],
        );

        let poll = operator.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(PollExecute::NeedsMore, poll);

        let poll = operator.unary_finalize(&mut states, 0);
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let poll = operator.unary_execute_out(&mut states, 0, &mut output);
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!(["group_a", "group_b", "group_c",], [4_i64, 2, 4],);
        assert_batches_eq(&expected, &output);

        let poll = operator.unary_execute_out(&mut states, 0, &mut output);
        assert_eq!(PollExecute::Exhausted, poll);
        assert_eq!(0, output.num_rows);
    }
}
