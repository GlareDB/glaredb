mod cross_product;
mod match_tracker;

use std::task::Context;

use cross_product::CrossProductState;
use glaredb_error::{DbError, Result};
use match_tracker::MatchTracker;
use parking_lot::Mutex;

use super::{
    BaseOperator,
    ExecuteOperator,
    ExecutionProperties,
    PollExecute,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ConcurrentColumnCollection,
    ParallelColumnCollectionScanState,
};
use crate::arrays::datatype::DataType;
use crate::execution::partition_wakers::PartitionWakers;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::selection_evaluator::SelectionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::logical::logical_join::JoinType;

#[derive(Debug)]
pub struct NestedLoopJoinOperatorState {
    collected: ConcurrentColumnCollection,
    inner: Mutex<StateInner>,
}

#[derive(Debug)]
struct StateInner {
    /// Number of build inputs we're still waiting on to complete.
    remaining_build_inputs: usize,
    /// Wakers for pending probes if we're still building.
    probe_wakers: PartitionWakers,
    /// Rows in the left collection that matched.
    ///
    /// This is relative to the entire collection.
    ///
    /// Only used for LEFT/OUTER joins.
    left_matches: MatchTracker,
}

#[derive(Debug)]
pub struct NestedLoopJoinBuildState {
    append_state: ColumnCollectionAppendState,
}

#[derive(Debug)]
pub struct NestedLoopJoinProbeState {
    /// Index of this partition. Used to store a waker if needed.
    partition_idx: usize,
    /// If the build side is complete.
    build_complete: bool,
    /// Cross product state.
    cross_state: CrossProductState,
    /// Condition evaluator.
    evaluator: Option<SelectionEvaluator>,
    /// Rows in the right batch that matched.
    ///
    /// Only used for RIGHT/OUTER joins.
    right_matches: MatchTracker,
    /// State used to drain the left batches from the the collection.
    ///
    /// Only used for LEFT/OUTER joins.
    left_drain_state: ParallelColumnCollectionScanState,
    /// Marker for if this partition is currently draining for a LEFT JOIN.
    draining_left: bool,
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    pub(crate) join_type: JoinType,
    pub(crate) left_types: Vec<DataType>,
    #[expect(unused)] // For now.
    pub(crate) right_types: Vec<DataType>,
    pub(crate) output_types: Vec<DataType>,
    pub(crate) filter: Option<PhysicalScalarExpression>,
}

impl PhysicalNestedLoopJoin {
    pub fn new(
        join_type: JoinType,
        left_types: impl IntoIterator<Item = DataType>,
        right_types: impl IntoIterator<Item = DataType>,
        filter: Option<PhysicalScalarExpression>,
    ) -> Result<Self> {
        if !matches!(
            join_type,
            JoinType::Inner | JoinType::Right | JoinType::Left
        ) {
            return Err(DbError::new(format!(
                "Unsupported join type for nested loop join: {join_type}",
            )));
        }

        let left_types: Vec<_> = left_types.into_iter().collect();
        let right_types: Vec<_> = right_types.into_iter().collect();
        // TODO: Mark/semi/anti
        let output_types = left_types
            .iter()
            .cloned()
            .chain(right_types.iter().cloned())
            .collect();

        Ok(PhysicalNestedLoopJoin {
            join_type,
            left_types,
            right_types,
            output_types,
            filter,
        })
    }
}

impl BaseOperator for PhysicalNestedLoopJoin {
    type OperatorState = NestedLoopJoinOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let collection =
            ConcurrentColumnCollection::new(self.left_types.iter().cloned(), 1, props.batch_size);

        let inner = StateInner {
            remaining_build_inputs: 0, // Set when creating push partition states.
            probe_wakers: PartitionWakers::empty(), // Set when creating probe partition states.
            left_matches: MatchTracker::empty(),
        };

        Ok(NestedLoopJoinOperatorState {
            collected: collection,
            inner: Mutex::new(inner),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

/// Implementation of the "build" side of the join.
impl PushOperator for PhysicalNestedLoopJoin {
    type PartitionPushState = NestedLoopJoinBuildState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let states = (0..partitions)
            .map(|_| NestedLoopJoinBuildState {
                append_state: operator_state.collected.init_append_state(),
            })
            .collect();

        operator_state.inner.lock().remaining_build_inputs = partitions;

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        operator_state
            .collected
            .append_batch(&mut state.append_state, input)?;
        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        operator_state.collected.flush(&mut state.append_state)?;

        let mut inner = operator_state.inner.lock();
        inner.remaining_build_inputs -= 1;

        // If this is the last build input, go ahead and wake up all pending
        // probers.
        if inner.remaining_build_inputs == 0 {
            inner.probe_wakers.wake_all();

            // Init left matches if needed.
            if matches!(self.join_type, JoinType::Left) {
                let num_rows_left = operator_state.collected.flushed_rows();
                inner.left_matches.ensure_initialized(num_rows_left);
            }
        }

        Ok(PollFinalize::Finalized)
    }
}

/// Implementation of the "probe" side of the join.
impl ExecuteOperator for PhysicalNestedLoopJoin {
    type PartitionExecuteState = NestedLoopJoinProbeState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let mut inner = operator_state.inner.lock();
        inner.probe_wakers.init_for_partitions(partitions);

        // Init states for left drain. Note that these might end up being
        // unused, but creating them isn't expensive.
        let left_scan_states = operator_state
            .collected
            .init_parallel_scan_states(partitions);

        let states = left_scan_states
            .enumerate()
            .map(|(partition_idx, left_drain_state)| {
                let evaluator = match &self.filter {
                    Some(filter) => Some(SelectionEvaluator::try_new(
                        filter.clone(),
                        props.batch_size,
                    )?),
                    None => None,
                };

                Ok(NestedLoopJoinProbeState {
                    partition_idx,
                    build_complete: false,
                    cross_state: CrossProductState::new(self.left_types.iter().cloned())?,
                    evaluator,
                    right_matches: MatchTracker::empty(),
                    left_drain_state,
                    draining_left: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        if !state.build_complete {
            // Check operator state to see if we can continue.
            let mut inner = operator_state.inner.lock();
            if inner.remaining_build_inputs > 0 {
                // Still building, come back later.
                inner.probe_wakers.store(cx.waker(), state.partition_idx);
                return Ok(PollExecute::Pending);
            }

            // We can probe, avoid having to check the global state again.
            state.build_complete = true;
        }

        if state.draining_left {
            // We're just draining, input batch no longer matters.
            let count = operator_state.collected.parallel_scan(
                &state.cross_state.projections,
                &mut state.left_drain_state,
                &mut state.cross_state.batch,
            )?;
            if count == 0 {
                output.set_num_rows(0)?;
                return Ok(PollExecute::Exhausted);
            }

            // TODO: Don't lock.
            let inner = operator_state.inner.lock();
            // TODO: Possibly loop here if output rows is zero.
            inner.left_matches.left_outer_result(
                state.left_drain_state.state.relative_scan_offset(),
                &mut state.cross_state.batch,
                output,
            )?;

            // Keep going.
            return Ok(PollExecute::HasMore);
        }

        if matches!(self.join_type, JoinType::Right) {
            // Note this doesn't clear the existing values. This essentially
            // acts as an initialization step during new right-side batches.
            state.right_matches.ensure_initialized(input.num_rows());
        }

        loop {
            output.reset_for_write()?;

            let keep_right =
                state
                    .cross_state
                    .scan_next(&operator_state.collected, input, output)?;

            if !keep_right {
                // We're done scanning the right batch. Finish up any remaining
                // scanning and request a new batch.

                // TODO: Join types, etc.

                if matches!(self.join_type, JoinType::Right) {
                    // Find rows that didn't match from the right, and flush
                    // them out with null left side values.
                    state.right_matches.right_outer_result(input, output)?;
                    state.right_matches.reset();

                    return Ok(PollExecute::Ready);
                }

                // Need to move to next batch.
                return Ok(PollExecute::NeedsMore);
            }

            match &mut state.evaluator {
                Some(evaluator) => {
                    // Evaluate the selection on the output of the cross
                    // product.
                    let selection = evaluator.select(output)?;

                    if selection.is_empty() {
                        // Evaluated empty, reset the chunk and try again.
                        continue;
                    }

                    if matches!(self.join_type, JoinType::Right) {
                        // Mark right rows as matched.
                        state.right_matches.set_matches(selection.iter().copied());
                    }

                    if matches!(self.join_type, JoinType::Left) {
                        // Mark left _row_ as matched. Note that we produce a
                        // row at a time for the left.
                        let left_row =
                            state.cross_state.collection_scan_offset().ok_or_else(|| {
                                DbError::new("Expected in-progress cross product scan")
                            })?;
                        let mut inner = operator_state.inner.lock();
                        inner.left_matches.set_match(left_row);
                    }

                    // We have a selection, select the output.
                    output.select(selection.iter().copied())?;

                    return Ok(PollExecute::HasMore);
                }
                None => {
                    // Just normal cross product, output already has everything,
                    // so keep going with same input.
                    return Ok(PollExecute::HasMore);
                }
            }
        }
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            // Trigger left drain if needed.
            state.draining_left = true;
            return Ok(PollFinalize::NeedsDrain);
        }

        // Otherwise we're done.
        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalNestedLoopJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("NestedLoopJoin")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::exprs::plan_scalar;
    use crate::testutil::operator::OperatorWrapper;
    use crate::{expr, generate_batch};

    #[test]
    fn cross_join_single_partition() {
        let wrapper = OperatorWrapper::new(
            PhysicalNestedLoopJoin::new(JoinType::Inner, [DataType::Utf8], [DataType::Int32], None)
                .unwrap(),
        );

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut push_states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();
        let mut probe_states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        // Build
        let mut input = generate_batch!(["a", "b"]);
        let poll = wrapper
            .poll_push(&op_state, &mut push_states[0], &mut input)
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll);
        let poll = wrapper
            .poll_finalize_push(&op_state, &mut push_states[0])
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);

        // Probe
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 16).unwrap();

        let mut input = generate_batch!([1, 2]);
        let poll = wrapper
            .poll_execute(&op_state, &mut probe_states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!(["a", "a"], [1, 2]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(&op_state, &mut probe_states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!(["b", "b"], [1, 2]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn inner_join_single_eq_condition() {
        // CONDITION: a = b
        let mut list = TableList::empty();
        let t0 = list.push_table(None, [DataType::Int32], ["a"]).unwrap();
        let t1 = list.push_table(None, [DataType::Int32], ["b"]).unwrap();
        let expr = plan_scalar(
            &list,
            expr::eq(
                expr::column((t0, 0), DataType::Int32),
                expr::column((t1, 0), DataType::Int32),
            )
            .unwrap(),
        );

        let wrapper = OperatorWrapper::new(
            PhysicalNestedLoopJoin::new(
                JoinType::Inner,
                [DataType::Int32],
                [DataType::Int32],
                Some(expr),
            )
            .unwrap(),
        );

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut push_states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();
        let mut probe_states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        // Build
        let mut build_input = generate_batch!([5, 3, 1, 1]);
        let poll = wrapper
            .poll_push(&op_state, &mut push_states[0], &mut build_input)
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll);
        let poll = wrapper
            .poll_finalize_push(&op_state, &mut push_states[0])
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);

        // Probe
        let mut output = Batch::new([DataType::Int32, DataType::Int32], 16).unwrap();
        let mut probe_input = generate_batch!([1, 2, 3, 4]);

        let poll = wrapper
            .poll_execute(
                &op_state,
                &mut probe_states[0],
                &mut probe_input,
                &mut output,
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);
        let expected = generate_batch!([3], [3]);
        assert_batches_eq(&expected, &output);

        let poll = wrapper
            .poll_execute(
                &op_state,
                &mut probe_states[0],
                &mut probe_input,
                &mut output,
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);
        let expected = generate_batch!([1], [1]);
        assert_batches_eq(&expected, &output);

        // ... And so on
        //
        // Not optimizing the size of the batches here since equality predicates
        // should be going to hash join.
    }
}
