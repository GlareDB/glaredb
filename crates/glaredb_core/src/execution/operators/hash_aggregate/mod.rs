pub mod distinct_aggregates;

mod aggregate_hash_table;
mod grouping_set_hash_table;
mod grouping_value;

use std::collections::BTreeSet;
use std::task::Context;

use distinct_aggregates::{
    AggregateSelection,
    DistinctAggregateInfo,
    DistinctCollection,
    DistinctCollectionOperatorState,
    DistinctCollectionPartitionState,
};
use glaredb_error::{DbError, Result};
use grouping_set_hash_table::{
    GroupingSetHashTable,
    GroupingSetOperatorState,
    GroupingSetPartitionState,
};
use parking_lot::Mutex;

use super::util::delayed_count::DelayedPartitionCount;
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
    Aggregating(HashAggregateAggregatingPartitionState),
    AggregatingDistinct(HashAggregateAggregatingDistinctPartitionState),
    Scanning(HashAggregateScanningPartitionState),
}

#[derive(Debug)]
pub struct HashAggregateAggregatingPartitionState {
    partition_idx: usize,
    /// Partition state per grouping set table.
    states: Vec<GroupingSetPartitionState>,
    /// Distinct states per grouping set.
    distinct_states: Vec<DistinctCollectionPartitionState>,
}

#[derive(Debug)]
pub struct HashAggregateAggregatingDistinctPartitionState {
    partition_idx: usize,
    /// Partition state per grouping set table.
    states: Vec<GroupingSetPartitionState>,
    /// Distinct states per grouping set.
    distinct_states: Vec<DistinctCollectionPartitionState>,
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
    /// Batch size used when draining the distinct tables.
    batch_size: usize,
    /// Hash table per grouping set.
    tables: Vec<GroupingSetHashTable>,
    /// State for each grouping set hash table.
    table_states: Vec<GroupingSetOperatorState>,
    /// Distinct collections for each grouping set.
    distinct_collections: Vec<DistinctCollection>,
    /// Distinct state for each grouping set.
    distinct_states: Vec<DistinctCollectionOperatorState>,
    inner: Mutex<HashAggregateOperatoreStateInner>,
}

#[derive(Debug)]
struct HashAggregateOperatoreStateInner {
    /// Remaining partitions working on normal aggregates.
    remaining_normal: DelayedPartitionCount,
    /// Remaining partitions working on distinct aggregates.
    remaining_distinct: DelayedPartitionCount,
    /// Wakers waiting for normal aggregates to finish so we can compute the
    /// distinct aggregates.
    pending_distinct: PartitionWakers,
    /// Wakers waiting to scan the final aggregate tables.
    pending_drain: PartitionWakers,
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

        // Create distinct collection per grouping set.
        let distinct_collections = self
            .grouping_sets
            .iter()
            .map(|grouping_set| {
                // Need to distinct on groups too.
                let groups: Vec<_> = grouping_set
                    .iter()
                    .map(|&group_idx| self.aggregates.groups[group_idx].clone())
                    .collect();

                DistinctCollection::new(self.agg_selection.distinct.iter().map(|&idx| {
                    DistinctAggregateInfo {
                        inputs: &self.aggregates.aggregates[idx].columns,
                        groups: &groups,
                    }
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        let distinct_states = distinct_collections
            .iter()
            .map(|col| col.create_operator_state(props.batch_size))
            .collect::<Result<Vec<_>>>()?;

        let inner = HashAggregateOperatoreStateInner {
            remaining_normal: DelayedPartitionCount::uninit(),
            remaining_distinct: DelayedPartitionCount::uninit(),
            pending_distinct: PartitionWakers::empty(),
            pending_drain: PartitionWakers::empty(),
        };

        Ok(HashAggregateOperatorState {
            batch_size: props.batch_size,
            tables,
            table_states,
            distinct_collections,
            distinct_states,
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
                HashAggregateAggregatingPartitionState {
                    partition_idx: idx,
                    distinct_states: Vec::with_capacity(operator_state.tables.len()), // Populated below
                    states: Vec::with_capacity(operator_state.tables.len()), // Populated below
                }
            })
            .collect();

        let inner = &mut operator_state.inner.lock();
        inner.pending_drain.init_for_partitions(partitions);
        inner.pending_distinct.init_for_partitions(partitions);
        inner.remaining_normal.set(partitions)?;
        inner.remaining_distinct.set(partitions)?;

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

        debug_assert_eq!(
            operator_state.distinct_collections.len(),
            operator_state.distinct_states.len(),
        );

        // Generate distinct states for each partition
        for (distinct, op_state) in operator_state
            .distinct_collections
            .iter()
            .zip(&operator_state.distinct_states)
        {
            let distinct_partition_states =
                distinct.create_partition_states(op_state, partitions)?;

            debug_assert_eq!(partition_states.len(), distinct_partition_states.len());
            for (partition_state, distinct_state) in
                partition_states.iter_mut().zip(distinct_partition_states)
            {
                partition_state.distinct_states.push(distinct_state);
            }
        }

        let partition_states = partition_states
            .into_iter()
            .map(HashAggregatePartitionState::Aggregating)
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
            HashAggregatePartitionState::Aggregating(aggregating) => {
                debug_assert_eq!(aggregating.states.len(), operator_state.tables.len());

                // Update distinct states.
                for (distinct, state) in operator_state
                    .distinct_collections
                    .iter()
                    .zip(&mut aggregating.distinct_states)
                {
                    distinct.insert(state, input)?;
                }

                // Insert input into each grouping set table.
                for (table, state) in operator_state.tables.iter().zip(&mut aggregating.states) {
                    table.insert_input(state, &self.agg_selection.non_distinct, input)?;
                }

                Ok(PollExecute::NeedsMore)
            }
            HashAggregatePartitionState::AggregatingDistinct(aggregating) => {
                debug_assert_eq!(aggregating.states.len(), operator_state.tables.len());
                debug_assert_eq!(aggregating.distinct_states.len(), aggregating.states.len());

                let mut shared = operator_state.inner.lock();
                if shared.remaining_normal.current()? != 0 {
                    // Normal aggregates still happening, we don't have all
                    // distinct inputs yet, come back later.
                    shared
                        .pending_distinct
                        .store(cx.waker(), aggregating.partition_idx);
                    return Ok(PollExecute::Pending);
                }
                std::mem::drop(shared);

                // Drain all distinct tables.
                //
                // Top level loop: Distinct states per grouping set.
                // Second loop: Distinct tables within a group.
                for (grouping_set_idx, distinct) in
                    operator_state.distinct_collections.iter().enumerate()
                {
                    let distinct_op_state = &operator_state.distinct_states[grouping_set_idx];
                    let distinct_part_state = &mut aggregating.distinct_states[grouping_set_idx];

                    for table_idx in 0..distinct.num_distinct_tables() {
                        let mut batch = Batch::new(
                            distinct.iter_table_types(table_idx),
                            operator_state.batch_size,
                        )?;

                        // Figure out which aggs these inputs actually apply to.
                        let agg_sel: Vec<_> = distinct
                            .aggregates_for_table(table_idx)
                            .iter()
                            .map(|&distinct_agg_idx| {
                                // Distinct table only knows about distinct
                                // aggregates. Map that index back to the full
                                // aggregate list.
                                self.agg_selection.distinct[distinct_agg_idx]
                            })
                            .collect();

                        loop {
                            batch.reset_for_write()?;
                            distinct.scan(
                                distinct_op_state,
                                distinct_part_state,
                                table_idx,
                                &mut batch,
                            )?;

                            if batch.num_rows() == 0 {
                                // Move to next distinct table in this grouping set.
                                break;
                            }

                            // Now insert into the normal agg table.
                            operator_state.tables[grouping_set_idx].insert_for_distinct(
                                &mut aggregating.states[grouping_set_idx],
                                &agg_sel,
                                &mut batch,
                            )?;
                        }
                    }
                }

                // TODO: Move below the merge.
                let mut shared = operator_state.inner.lock();

                // Now merge into the global table.
                for (table_idx, table) in operator_state.tables.iter().enumerate() {
                    let _ = table.merge(
                        &operator_state.table_states[table_idx],
                        &mut aggregating.states[table_idx],
                    )?;
                }

                let remaining = shared.remaining_distinct.dec_by_one()?;

                if remaining == 0 {
                    // Wake up any pending drainers.
                    shared.pending_drain.wake_all();
                }

                // See finalize.
                let table_states: Vec<_> = aggregating.states.drain(..).enumerate().collect();
                // Set self to begin draining.
                *state =
                    HashAggregatePartitionState::Scanning(HashAggregateScanningPartitionState {
                        partition_idx: aggregating.partition_idx,
                        scan_ready: false,
                        states: table_states,
                    });

                output.set_num_rows(0)?;
                // Call us again.
                Ok(PollExecute::HasMore)
            }
            HashAggregatePartitionState::Scanning(scanning) => {
                if !scanning.scan_ready {
                    // Check operator state to really see if the scan is ready
                    // or not.
                    let mut shared_state = operator_state.inner.lock();
                    // 'remaining_distinct' always updated even when we don't
                    // have distinct aggregates.
                    let scan_ready = shared_state.remaining_normal.current()? == 0
                        && shared_state.remaining_distinct.current()? == 0;
                    if !scan_ready {
                        // Come back later.
                        shared_state
                            .pending_drain
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
            HashAggregatePartitionState::Aggregating(building) => {
                // Finalize the building for this partition by merging all
                // partition-local tables into the operator tables.

                // TODO: Figure out how to not need this here.
                let mut shared_state = operator_state.inner.lock();

                // Merge the distinct collections.
                for (idx, distinct) in operator_state.distinct_collections.iter().enumerate() {
                    let op_state = &operator_state.distinct_states[idx];
                    let part_state = &mut building.distinct_states[idx];
                    distinct.merge(op_state, part_state)?;
                }

                if self.agg_selection.distinct.is_empty() {
                    // We only have normal aggregates. We can merge our tables
                    // and jump straight to scan.

                    // Merge non-distinct aggs to global table.
                    for (table_idx, table) in operator_state.tables.iter().enumerate() {
                        let _ = table.merge(
                            &operator_state.table_states[table_idx],
                            &mut building.states[table_idx],
                        )?;
                    }

                    // Attach table indices to the states. We're going to drain the
                    // states as a queue during draining, so we need to preserve the
                    // table index the state is for.
                    let table_states: Vec<_> = building.states.drain(..).enumerate().collect();
                    *state = HashAggregatePartitionState::Scanning(
                        HashAggregateScanningPartitionState {
                            partition_idx: building.partition_idx,
                            scan_ready: false,
                            states: table_states,
                        },
                    );

                    let remaining = shared_state.remaining_normal.dec_by_one()?;
                    // Decremtn the the pending distinct count too so we can
                    // simplify the check in drain.
                    let _ = shared_state.remaining_distinct.dec_by_one()?;

                    if remaining == 0 {
                        // Wake up all partitions, we're ready to produce results.
                        shared_state.pending_drain.wake_all();
                    }

                    Ok(PollFinalize::NeedsDrain)
                } else {
                    // We have distinct aggregates. We need to drain the
                    // distinct tables and update our local agg states before
                    // merging with the global states.

                    // Note we're not merging "normal" aggs yet since we can
                    // only merge the table once. We do that once we complete
                    // computing the distinct aggs.

                    let states = std::mem::take(&mut building.states);
                    let distinct_states = std::mem::take(&mut building.distinct_states);

                    *state = HashAggregatePartitionState::AggregatingDistinct(
                        HashAggregateAggregatingDistinctPartitionState {
                            partition_idx: building.partition_idx,
                            states,
                            distinct_states,
                        },
                    );

                    let remaining = shared_state.remaining_normal.dec_by_one()?;

                    if remaining == 0 {
                        // Wake up any partition waiting on all distinct inputs.
                        shared_state.pending_distinct.wake_all();
                    }

                    Ok(PollFinalize::NeedsDrain)
                }
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
