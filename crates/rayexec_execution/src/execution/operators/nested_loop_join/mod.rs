mod cross_product;

use std::task::Context;

use cross_product::CrossProductState;
use parking_lot::Mutex;
use rayexec_error::Result;

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
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    pub(crate) join_type: JoinType,
    pub(crate) left_types: Vec<DataType>,
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
    ) -> Self {
        let left_types: Vec<_> = left_types.into_iter().collect();
        let right_types: Vec<_> = right_types.into_iter().collect();
        // TODO: Mark/semi/anti
        let output_types = left_types
            .iter()
            .cloned()
            .chain(right_types.iter().cloned())
            .collect();

        PhysicalNestedLoopJoin {
            join_type,
            left_types,
            right_types,
            output_types,
            filter,
        }
    }
}

impl BaseOperator for PhysicalNestedLoopJoin {
    type OperatorState = NestedLoopJoinOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        let collection =
            ConcurrentColumnCollection::new(self.left_types.iter().cloned(), 16, props.batch_size);

        let inner = StateInner {
            remaining_build_inputs: 0, // Set when creating push partition states.
            probe_wakers: PartitionWakers::empty(), // Set when creating probe partition states.
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
        operator_state
            .inner
            .lock()
            .probe_wakers
            .init_for_partitions(partitions);

        let states = (0..partitions)
            .map(|partition_idx| {
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

        loop {
            let keep_scanning =
                state
                    .cross_state
                    .try_next(&operator_state.collected, input, output)?;

            if !keep_scanning {
                // TODO: Join types, etc.

                // Need to move to next batch.
                return Ok(PollExecute::NeedsMore);
            }

            match &mut state.evaluator {
                Some(evaluator) => {
                    //
                    unimplemented!()
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
        _state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        // TODO: Left join drains.
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
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn cross_join_single_partition() {
        let wrapper = OperatorWrapper::new(PhysicalNestedLoopJoin::new(
            JoinType::Inner,
            [DataType::Utf8],
            [DataType::Int32],
            None,
        ));

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
}
