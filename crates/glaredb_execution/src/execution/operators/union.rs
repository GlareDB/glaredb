use std::sync::Arc;
use std::task::{Context, Waker};

use glaredb_error::Result;
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
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct UnionOperatorState {
    shared: Mutex<Vec<Arc<Mutex<SharedPartitionState>>>>,
}

#[derive(Debug)]
struct SharedPartitionState {
    /// Indicates if this batch is currently buffering data from the left side.
    has_data: bool,
    /// If we're done on the push side.
    push_finished: bool,
    /// The buffered batch.
    buffer: Batch,
    /// The pull side waker.
    pull_waker: Option<Waker>,
    /// The push side waker.
    push_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct UnionPushPartitionState {
    shared: Arc<Mutex<SharedPartitionState>>,
}

#[derive(Debug)]
pub struct UnionExecutePartitionState {
    draining: bool,
    shared: Arc<Mutex<SharedPartitionState>>,
}

#[derive(Debug)]
pub struct PhysicalUnion {
    pub(crate) datatypes: Vec<DataType>,
}

impl PhysicalUnion {
    pub fn new(datatypes: impl IntoIterator<Item = DataType>) -> Self {
        PhysicalUnion {
            datatypes: datatypes.into_iter().collect(),
        }
    }

    fn ensure_shared_states(
        &self,
        shared_states: &mut Vec<Arc<Mutex<SharedPartitionState>>>,
        batch_size: usize,
        partitions: usize,
    ) -> Result<()> {
        if !shared_states.is_empty() {
            // States already created.
            // TODO: Might make sense to validate the length.
            return Ok(());
        }

        for _ in 0..partitions {
            let shared = Arc::new(Mutex::new(SharedPartitionState {
                has_data: false,
                push_finished: false,
                buffer: Batch::new(self.datatypes.clone(), batch_size)?,
                pull_waker: None,
                push_waker: None,
            }));
            shared_states.push(shared);
        }

        Ok(())
    }
}

impl BaseOperator for PhysicalUnion {
    type OperatorState = UnionOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(UnionOperatorState {
            // Shared states are created when we create the push states. Then
            // when creating the pull states, we clone the shared state for each
            // partition.
            shared: Mutex::new(Vec::new()),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
    }
}

impl PushOperator for PhysicalUnion {
    type PartitionPushState = UnionPushPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let mut shared_states = operator_state.shared.lock();
        self.ensure_shared_states(shared_states.as_mut(), props.batch_size, partitions)?;

        let states = (0..partitions)
            .map(|idx| UnionPushPartitionState {
                shared: shared_states[idx].clone(),
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        let mut shared = state.shared.lock();
        if shared.has_data {
            // Come back later.
            shared.push_waker = Some(cx.waker().clone());
            return Ok(PollPush::Pending);
        }

        // Copy in the data.
        shared.buffer.swap(input)?;
        shared.has_data = true;

        if let Some(waker) = shared.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        let mut shared = state.shared.lock();
        shared.push_finished = true;

        if let Some(waker) = shared.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
    }
}

impl ExecuteOperator for PhysicalUnion {
    type PartitionExecuteState = UnionExecutePartitionState;

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        let mut shared_states = operator_state.shared.lock();
        self.ensure_shared_states(shared_states.as_mut(), props.batch_size, partitions)?;

        let states = (0..partitions)
            .map(|idx| UnionExecutePartitionState {
                draining: false,
                shared: shared_states[idx].clone(),
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        if !state.draining {
            // Just pass the batch right on through.
            output.swap(input)?;
            return Ok(PollExecute::Ready);
        }

        // Otherwise check the shared state to see if we have anything.
        let mut shared = state.shared.lock();
        if !shared.has_data {
            if shared.push_finished {
                // We're done.
                output.set_num_rows(0)?;
                return Ok(PollExecute::Exhausted);
            }

            // Come back later.
            shared.pull_waker = Some(cx.waker().clone());
            if let Some(waker) = shared.push_waker.take() {
                waker.wake();
            }

            return Ok(PollExecute::Pending);
        }

        // We have data from the other side, take it.
        output.swap(&mut shared.buffer)?;
        shared.has_data = false;

        if let Some(waker) = shared.push_waker.take() {
            waker.wake();
        }

        // Keep coming back to drain data from the other side.
        Ok(PollExecute::HasMore)
    }

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
    ) -> Result<PollFinalize> {
        state.draining = true;
        Ok(PollFinalize::NeedsDrain)
    }
}

impl Explainable for PhysicalUnion {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Union")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn single_partition_exhaust_right_exhaust_left() {
        let wrapper = OperatorWrapper::new(PhysicalUnion::new([DataType::Int32, DataType::Utf8]));

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();

        let mut push_states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();
        let mut exec_states = wrapper
            .operator
            .create_partition_execute_states(&op_state, props, 1)
            .unwrap();

        let mut out = Batch::new([DataType::Int32, DataType::Utf8], 16).unwrap();

        // Push on right first.
        let mut right_input = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        let poll = wrapper
            .poll_execute(&op_state, &mut exec_states[0], &mut right_input, &mut out)
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected = generate_batch!([1, 2, 3], ["a", "b", "c"]);
        assert_batches_eq(&expected, &out);

        // Finalize right.
        let poll = wrapper
            .poll_finalize_execute(&op_state, &mut exec_states[0])
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        // Push left.
        let mut left_input = generate_batch!([4, 5], ["d", "e"]);
        let poll = wrapper
            .poll_push(&op_state, &mut push_states[0], &mut left_input)
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll);

        // Finalize left.
        let poll = wrapper
            .poll_finalize_push(&op_state, &mut push_states[0])
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);

        // Drain using right.
        let poll = wrapper
            .poll_execute(&op_state, &mut exec_states[0], &mut right_input, &mut out)
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected = generate_batch!([4, 5], ["d", "e"]);
        assert_batches_eq(&expected, &out);

        // Exhuast
        let poll = wrapper
            .poll_execute(&op_state, &mut exec_states[0], &mut right_input, &mut out)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);
        assert_eq!(0, out.num_rows());
    }
}
