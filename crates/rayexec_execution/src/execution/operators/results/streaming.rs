use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures::Stream;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::execution::operators::{
    BaseOperator,
    ExecutionProperties,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Streams result batches for a query.
///
/// This produces a single stream from some number of partitions. The ordering
/// of batches is not guaranteed.
#[derive(Debug, Clone)]
pub struct ResultStream {
    inner: Arc<Mutex<ResultStreamInner>>,
}

#[derive(Debug)]
struct ResultStreamInner {
    /// Batch that needs to be sent on the stream.
    buffered: Option<Batch>,
    /// Waker for the single pull side.
    pull_waker: Option<Waker>,
    /// Wakers for all partitions on the push side.
    ///
    /// Resized when we create the partition states.
    push_wakers: Vec<Option<Waker>>,
    /// Query error.
    error: Option<RayexecError>,
    /// Number of inputs we're waiting on to finish.
    ///
    /// Set when we create partition states.
    remaining_inputs: usize,
}

impl ResultStream {
    pub fn new() -> Self {
        ResultStream {
            inner: Arc::new(Mutex::new(ResultStreamInner {
                buffered: None,
                pull_waker: None,
                push_wakers: Vec::new(),
                error: None,
                remaining_inputs: 0,
            })),
        }
    }

    /// Set the error for the stream.
    pub fn set_error(&self, error: RayexecError) {
        let mut inner = self.inner.lock();
        inner.error = Some(error);

        if let Some(waker) = inner.pull_waker.take() {
            waker.wake();
        }
    }
}

impl Stream for ResultStream {
    type Item = Result<Batch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();
        // Did we error?
        if let Some(error) = inner.error.take() {
            return Poll::Ready(Some(Err(error)));
        }

        if let Some(batch) = inner.buffered.take() {
            // Wake up all inputs.
            //
            // TODO: Do we want to store all wakers? Currently this operator is
            // able to terminate a query multiple partition pipelines, but that
            // requires that we have a waker per partition.
            for waker in &mut inner.push_wakers {
                if let Some(push_waker) = waker.take() {
                    push_waker.wake();
                }
            }

            return Poll::Ready(Some(Ok(batch)));
        }

        // No batch, see if we're finished.
        if inner.remaining_inputs == 0 {
            return Poll::Ready(None);
        }

        // Need to wait for the next batch.
        inner.pull_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[derive(Debug)]
pub struct StreamingResultsOperatorState {
    stream: ResultStream,
}

#[derive(Debug)]
pub struct StreamingResultsPartitionState {
    partition_idx: usize,
}

#[derive(Debug)]
pub struct PhysicalStreamingResults {
    pub(crate) stream: ResultStream,
}

impl PhysicalStreamingResults {
    pub fn new(stream: ResultStream) -> Self {
        PhysicalStreamingResults { stream }
    }
}

impl BaseOperator for PhysicalStreamingResults {
    type OperatorState = StreamingResultsOperatorState;

    fn create_operator_state(
        &self,
        _context: &DatabaseContext,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(StreamingResultsOperatorState {
            stream: self.stream.clone(),
        })
    }

    fn output_types(&self) -> &[DataType] {
        // TODO: ?
        &[]
    }
}

impl PushOperator for PhysicalStreamingResults {
    type PartitionPushState = StreamingResultsPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        // Resize wakers vec.
        let mut inner = operator_state.stream.inner.lock();
        inner.push_wakers.resize(partitions, None);
        inner.remaining_inputs = partitions;

        let states = (0..partitions)
            .map(|partition_idx| StreamingResultsPartitionState { partition_idx })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        state: &mut Self::PartitionPushState,
        operator_state: &Self::OperatorState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        let mut inner = operator_state.stream.inner.lock();

        if inner.buffered.is_some() {
            // Batch already being buffered, come back later.
            inner.push_wakers[state.partition_idx] = Some(cx.waker().clone());
            if let Some(waker) = inner.pull_waker.take() {
                waker.wake();
            }

            return Ok(PollPush::Pending);
        }

        let buffered = Batch::new_from_other(input)?;
        inner.buffered = Some(buffered);

        if let Some(waker) = inner.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _state: &mut Self::PartitionPushState,
        operator_state: &Self::OperatorState,
    ) -> Result<PollFinalize> {
        let mut inner = operator_state.stream.inner.lock();
        inner.remaining_inputs -= 1;

        if let Some(waker) = inner.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalStreamingResults {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("StreamingResults")
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use stdutil::task::noop_context;

    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::database_context::test_database_context;
    use crate::testutil::operator::{CountingWaker, OperatorWrapper};

    #[test]
    fn single_partition_stream() {
        let mut stream = ResultStream::new();
        let wrapper = OperatorWrapper::new(PhysicalStreamingResults::new(stream.clone()));

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper
            .operator
            .create_operator_state(&test_database_context(), props)
            .unwrap();
        let mut states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();

        let mut input = generate_batch!([1, 2, 3, 4]);
        let poll = wrapper
            .poll_push(&mut states[0], &op_state, &mut input)
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll);

        // Should be able to pull from stream.
        match stream.poll_next_unpin(&mut noop_context()) {
            Poll::Ready(Some(Ok(batch))) => {
                let expected = generate_batch!([1, 2, 3, 4]);
                assert_batches_eq(&expected, &batch);
            }
            other => panic!("unexpected stream output: {other:?}"),
        }
    }

    #[test]
    fn single_partition_push_wakes_pull() {
        let mut stream = ResultStream::new();
        let wrapper = OperatorWrapper::new(PhysicalStreamingResults::new(stream.clone()));

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper
            .operator
            .create_operator_state(&test_database_context(), props)
            .unwrap();
        let mut states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();

        let stream_waker = Arc::new(CountingWaker::default());
        let waker = Waker::from(stream_waker.clone()); // Needs let binding.
        let mut stream_cx = Context::from_waker(&waker);

        // Try pulling first.
        assert_eq!(0, stream_waker.wake_count());
        let poll = stream.poll_next_unpin(&mut stream_cx);
        assert!(poll.is_pending(), "unexpected poll: {poll:?}");

        // Now push.
        let mut input = generate_batch!([1, 2, 3, 4]);
        let poll = wrapper
            .poll_push(&mut states[0], &op_state, &mut input)
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll);

        // Should have woken the stream.
        assert_eq!(1, stream_waker.wake_count());
    }
}
