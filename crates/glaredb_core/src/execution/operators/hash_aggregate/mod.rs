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
use glaredb_error::{DbError, OptionExt, Result};
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
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
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
    /// Partition is inserting values into its local tables.
    Aggregating(HashAggregateAggregatingPartitionState),
    /// Partition is merging a subset of the distinct tables.
    MergingDistinct(HashAggregateMergingDistinctPartitionState),
    /// Partition is scanning from the global distinct tables and writing values
    /// to its local aggregate hash tables.
    AggregatingDistinct(HashAggregateAggregatingDistinctPartitionState),
    /// Partition is merg a subset of the global aggregate tables.
    Merging(HashAggregateMergingPartitionState),
    /// Partition is scanning.
    Scanning(HashAggregateScanningPartitionState),
}

#[derive(Debug)]
pub struct HashAggregateAggregatingPartitionState {
    inner: AggregatingPartitionState,
}

#[derive(Debug)]
pub struct HashAggregateMergingDistinctPartitionState {
    inner: AggregatingPartitionState,
    /// Queue of distinct tables that this partition is responsible for merging.
    ///
    /// Values corresponds to the grouping set index.
    distinct_tables_queue: Vec<usize>,
}

#[derive(Debug)]
pub struct HashAggregateAggregatingDistinctPartitionState {
    inner: AggregatingPartitionState,
}

#[derive(Debug)]
pub struct HashAggregateMergingPartitionState {
    inner: AggregatingPartitionState,
    /// Queue of tables that this partition is responsible for merging.
    ///
    /// Values corresponds to the grouping set index.
    tables_queue: Vec<usize>,
}

#[derive(Debug)]
struct AggregatingPartitionState {
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
    /// Total number of partitions.
    partition_count: Option<usize>,
    /// Remaining partitions working on normal aggregates.
    remaining_normal: DelayedPartitionCount,
    /// Remaining partitions working on merging the distinct tables.
    remaining_distinct_mergers: DelayedPartitionCount,
    /// Remaining partitions working on distinct aggregates.
    remaining_distinct_aggregators: DelayedPartitionCount,
    /// Remaining partitions working on merging the aggregate tables.
    remaining_mergers: DelayedPartitionCount,
    /// Partitions waiting for normal aggregates to finish so we can merge the final
    /// distinct tables.
    pending_distinct_mergers: PartitionWakers,
    /// Partitions waiting on the distinct merges to complete before scanning
    /// the the distinct tables.
    pending_distinct_aggregators: PartitionWakers,
    /// Partitions waiting for the distinct aggregates to finish before
    /// producing the final aggregate tables.
    pending_mergers: PartitionWakers,
    /// Wakers waiting to scan the final aggregate tables.
    pending_drainers: PartitionWakers,
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
            partition_count: None, // Updated when we create partition states.
            remaining_normal: DelayedPartitionCount::uninit(),
            remaining_distinct_mergers: DelayedPartitionCount::uninit(),
            remaining_distinct_aggregators: DelayedPartitionCount::uninit(),
            remaining_mergers: DelayedPartitionCount::uninit(),
            pending_distinct_mergers: PartitionWakers::empty(),
            pending_distinct_aggregators: PartitionWakers::empty(),
            pending_mergers: PartitionWakers::empty(),
            pending_drainers: PartitionWakers::empty(),
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
                    inner: AggregatingPartitionState {
                        partition_idx: idx,
                        distinct_states: Vec::with_capacity(operator_state.tables.len()), // Populated below
                        states: Vec::with_capacity(operator_state.tables.len()), // Populated below
                    },
                }
            })
            .collect();

        let inner = &mut operator_state.inner.lock();
        inner.partition_count = Some(partitions);

        // Wakers.
        inner.pending_drainers.init_for_partitions(partitions);
        inner
            .pending_distinct_mergers
            .init_for_partitions(partitions);
        inner
            .pending_distinct_aggregators
            .init_for_partitions(partitions);
        inner.pending_mergers.init_for_partitions(partitions);

        // Delayed counts.
        inner.remaining_normal.set(partitions)?;
        inner.remaining_distinct_mergers.set(partitions)?;
        inner.remaining_distinct_aggregators.set(partitions)?;
        inner.remaining_mergers.set(partitions)?;

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
                partition_state.inner.states.push(table_state);
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
                partition_state.inner.distinct_states.push(distinct_state);
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
                debug_assert_eq!(aggregating.inner.states.len(), operator_state.tables.len());

                // Update distinct states.
                for (distinct, state) in operator_state
                    .distinct_collections
                    .iter()
                    .zip(&mut aggregating.inner.distinct_states)
                {
                    distinct.insert(state, input)?;
                }

                // Insert input into each grouping set table.
                for (table, state) in operator_state
                    .tables
                    .iter()
                    .zip(&mut aggregating.inner.states)
                {
                    table.insert_input_loca(state, &self.agg_selection.non_distinct, input)?;
                }

                Ok(PollExecute::NeedsMore)
            }
            HashAggregatePartitionState::MergingDistinct(merging) => {
                let mut shared = operator_state.inner.lock();
                if shared.remaining_normal.current()? != 0 {
                    // Normal aggregates still going, we don't have all distinct
                    // inputs yet. Come back later.
                    shared
                        .pending_distinct_mergers
                        .store(cx.waker(), merging.inner.partition_idx);
                    return Ok(PollExecute::Pending);
                }
                std::mem::drop(shared);

                debug_assert_eq!(
                    operator_state.distinct_collections.len(),
                    operator_state.distinct_states.len()
                );

                // We have all inputs. Go ahead and merge the distinct tables
                // this partition is responsible for.
                while let Some(idx) = merging.distinct_tables_queue.pop() {
                    operator_state.distinct_collections[idx]
                        .merge_flushed(&operator_state.distinct_states[idx])?;
                }

                // Update our state to scan the distinct values.
                let states = std::mem::take(&mut merging.inner.states);
                let distinct_states = std::mem::take(&mut merging.inner.distinct_states);
                *state = HashAggregatePartitionState::AggregatingDistinct(
                    HashAggregateAggregatingDistinctPartitionState {
                        inner: AggregatingPartitionState {
                            partition_idx: merging.inner.partition_idx,
                            states,
                            distinct_states,
                        },
                    },
                );

                let mut shared = operator_state.inner.lock();
                let remaining = shared.remaining_distinct_mergers.dec_by_one()?;
                if remaining == 0 {
                    // We were the last partition to complete merging, wake
                    // everyone else up.
                    shared.pending_distinct_aggregators.wake_all();
                }

                // Trigger re-poll
                output.set_num_rows(0)?;
                Ok(PollExecute::HasMore)
            }
            HashAggregatePartitionState::AggregatingDistinct(aggregating) => {
                debug_assert_eq!(aggregating.inner.states.len(), operator_state.tables.len());
                debug_assert_eq!(
                    aggregating.inner.distinct_states.len(),
                    aggregating.inner.states.len()
                );

                let mut shared = operator_state.inner.lock();
                if shared.remaining_distinct_mergers.current()? != 0 {
                    // Distinct mergers still happening, come back later when
                    // the merges are done.
                    shared
                        .pending_distinct_aggregators
                        .store(cx.waker(), aggregating.inner.partition_idx);
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
                    let distinct_part_state =
                        &mut aggregating.inner.distinct_states[grouping_set_idx];

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

                            // Now insert into our local tables.
                            operator_state.tables[grouping_set_idx].insert_for_distinct_local(
                                &mut aggregating.inner.states[grouping_set_idx],
                                &agg_sel,
                                &mut batch,
                            )?;
                        }
                    }
                }

                // Now flush into the global table.
                for (table_idx, table) in operator_state.tables.iter().enumerate() {
                    let _ = table.flush(
                        &operator_state.table_states[table_idx],
                        &mut aggregating.inner.states[table_idx],
                    )?;
                }

                let mut shared = operator_state.inner.lock();
                let remaining = shared.remaining_distinct_aggregators.dec_by_one()?;

                let num_partitions = shared.partition_count.required("partition count")?;

                // Update our state to begin merging the final tables.
                let states = std::mem::take(&mut aggregating.inner.states);
                let distinct_states = std::mem::take(&mut aggregating.inner.distinct_states);
                *state = HashAggregatePartitionState::Merging(HashAggregateMergingPartitionState {
                    inner: AggregatingPartitionState {
                        partition_idx: aggregating.inner.partition_idx,
                        states,
                        distinct_states,
                    },
                    // Generate table indices that this partition will be
                    // responsible for merging.
                    tables_queue: (0..operator_state.tables.len())
                        .filter(|idx| idx % num_partitions == aggregating.inner.partition_idx)
                        .collect(),
                });

                if remaining == 0 {
                    // Wake up any pending mergers.
                    shared.pending_drainers.wake_all();
                }

                // Call us again.
                output.set_num_rows(0)?;
                Ok(PollExecute::HasMore)
            }
            HashAggregatePartitionState::Merging(merging) => {
                let mut shared = operator_state.inner.lock();
                let is_ready = if self.agg_selection.distinct.is_empty() {
                    // No distinct aggregates, we just need to the normal
                    // aggregates to have completed.
                    shared.remaining_normal.current()? == 0
                } else {
                    // We have distinct aggregates, we need those to have
                    // completed before merging.
                    shared.remaining_distinct_aggregators.current()? == 0
                };
                if !is_ready {
                    // Not all partitions have completed writing distinct values
                    // to their tables. Come back later.
                    shared
                        .pending_mergers
                        .store(cx.waker(), merging.inner.partition_idx);
                    return Ok(PollExecute::Pending);
                }
                std::mem::drop(shared);

                debug_assert_eq!(
                    operator_state.tables.len(),
                    operator_state.table_states.len()
                );

                // We have all inputs. Go ahead and merge the tables this
                // partition is responsible for.
                while let Some(idx) = merging.tables_queue.pop() {
                    operator_state.tables[idx].merge_flushed(&operator_state.table_states[idx])?;
                }

                // Update our state for draining from the tables now.
                //
                // Attach table indices to the states. We're going to drain the
                // states as a queue during draining, so we need to preserve the
                // table index the state is for.
                let table_states: Vec<_> = merging.inner.states.drain(..).enumerate().collect();
                *state =
                    HashAggregatePartitionState::Scanning(HashAggregateScanningPartitionState {
                        partition_idx: merging.inner.partition_idx,
                        scan_ready: false,
                        states: table_states,
                    });

                let mut shared = operator_state.inner.lock();
                let remaining = shared.remaining_mergers.dec_by_one()?;
                if remaining == 0 {
                    // Wake up drainers.
                    shared.pending_drainers.wake_all();
                }

                // Trigger re-poll.
                output.set_num_rows(0)?;
                Ok(PollExecute::HasMore)
            }
            HashAggregatePartitionState::Scanning(scanning) => {
                if !scanning.scan_ready {
                    // Check operator state to really see if the scan is ready
                    // or not.
                    let mut shared_state = operator_state.inner.lock();
                    let scan_ready = shared_state.remaining_mergers.current()? == 0;
                    if !scan_ready {
                        // Come back later.
                        shared_state
                            .pending_drainers
                            .store(cx.waker(), scanning.partition_idx);
                        return Ok(PollExecute::Pending);
                    }

                    // We're good to scan, continue on...
                    //
                    // Stored on the partition state to avoid needing to check
                    // the operator state if scanning requires multiple polls
                    // (large number of group values).
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
                // Flush the distinct collections.
                for (idx, distinct) in operator_state.distinct_collections.iter().enumerate() {
                    let op_state = &operator_state.distinct_states[idx];
                    let part_state = &mut building.inner.distinct_states[idx];
                    distinct.flush(op_state, part_state)?;
                }

                let mut shared = operator_state.inner.lock();
                // Decrement the normal aggregate count.
                let _ = shared.remaining_normal.dec_by_one()?;

                let num_partitions = shared.partition_count.required("partition count")?;

                if self.agg_selection.distinct.is_empty() {
                    // We only have normal aggregates. We can merge our tables
                    // and jump straight to scan.

                    // Flush non-distinct aggs to global table.
                    for (table_idx, table) in operator_state.tables.iter().enumerate() {
                        let _ = table.flush(
                            &operator_state.table_states[table_idx],
                            &mut building.inner.states[table_idx],
                        )?;
                    }

                    // Jump to the merging state.
                    let states = std::mem::take(&mut building.inner.states);
                    let distinct_states = std::mem::take(&mut building.inner.distinct_states);
                    *state =
                        HashAggregatePartitionState::Merging(HashAggregateMergingPartitionState {
                            inner: AggregatingPartitionState {
                                partition_idx: building.inner.partition_idx,
                                states,
                                distinct_states,
                            },
                            // Generate table indices that this partition will be
                            // responsible for merging.
                            tables_queue: (0..operator_state.tables.len())
                                .filter(|idx| idx % num_partitions == building.inner.partition_idx)
                                .collect(),
                        });

                    // Now try draining.
                    //
                    // This will jump to the merging state. If this isn't the
                    // last partition, it will register a waker.
                    Ok(PollFinalize::NeedsDrain)
                } else {
                    // We have distinct aggregates. We need to drain the
                    // distinct tables and update our local agg states before
                    // merging with the global states.
                    //
                    // We **do not** flush our aggregate tables to the global
                    // table here.
                    //
                    // Instead we want this partition to take part in merging
                    // the distinct tables. Then once that's done, it'll jump to
                    // the AggregatingDistinct state which will scan a disjoint
                    // set of rows from the distinct tables and write it to its
                    // local aggregate tables.
                    //
                    // _Then_ it will flush to the global state before jumping
                    // to the draining state.

                    // Jump to distinct merging.
                    let states = std::mem::take(&mut building.inner.states);
                    let distinct_states = std::mem::take(&mut building.inner.distinct_states);
                    *state = HashAggregatePartitionState::MergingDistinct(
                        HashAggregateMergingDistinctPartitionState {
                            inner: AggregatingPartitionState {
                                partition_idx: building.inner.partition_idx,
                                states,
                                distinct_states,
                            },
                            // Generate distinct table indices that this
                            // partition will be responsible for merging.
                            distinct_tables_queue: (0..operator_state.tables.len())
                                .filter(|idx| idx % num_partitions == building.inner.partition_idx)
                                .collect(),
                        },
                    );

                    // Now draing.
                    //
                    // This will jump to the distinct merging state, and will
                    // register a waker if we having finished flushing the
                    // distinct tables.
                    Ok(PollFinalize::NeedsDrain)
                }
            }
            _ => Err(DbError::new("Hash aggregate partition already finalized")),
        }
    }
}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        // TODO: Grouping sets
        EntryBuilder::new(Self::OPERATOR_NAME, conf)
            .with_values(
                "aggregates",
                self.aggregates
                    .aggregates
                    .iter()
                    .map(|agg| agg.function.name),
            )
            .with_values("groups", &self.aggregates.groups)
            .build()
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
        // First poll is for the merge...
        assert_eq!(0, output.num_rows());

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
