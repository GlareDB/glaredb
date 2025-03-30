use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures::Stream;
use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::execution::operators::{
    BaseOperator,
    ExecutionProperties,
    PollFinalize,
    PollPush,
    PushOperator,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::runtime::ErrorSink;

/// Streams result batches for a query.
///
/// This produces a single stream from some number of partitions. The ordering
/// of batches is not guaranteed.
#[derive(Debug)]
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
    error: Option<DbError>,
    /// Number of inputs we're waiting on to finish.
    ///
    /// Set when we create partition states.
    remaining_inputs: usize,
}

impl Default for ResultStream {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn sink(&self) -> ResultSink {
        ResultSink {
            inner: self.inner.clone(),
        }
    }

    pub fn error_sink(&self) -> ResultErrorSink {
        ResultErrorSink {
            inner: self.inner.clone(),
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

/// The sink side of the stream. Inner is accessed directly in the operator.
#[derive(Debug, Clone)]
pub struct ResultSink {
    inner: Arc<Mutex<ResultStreamInner>>,
}

#[derive(Debug, Clone)]
pub struct ResultErrorSink {
    inner: Arc<Mutex<ResultStreamInner>>,
}

impl ErrorSink for ResultErrorSink {
    fn set_error(&self, error: DbError) {
        let mut inner = self.inner.lock();
        inner.error = Some(error);

        if let Some(waker) = inner.pull_waker.take() {
            waker.wake();
        }
    }
}

#[derive(Debug)]
pub struct StreamingResultsOperatorState {
    sink: ResultSink,
}

#[derive(Debug)]
pub struct StreamingResultsPartitionState {
    finished: bool, // For debug.
    partition_idx: usize,
}

/// Operator for producing non-empty batches on a stream.
#[derive(Debug)]
pub struct PhysicalStreamingResults {
    pub(crate) sink: ResultSink,
}

impl PhysicalStreamingResults {
    pub fn new(sink: ResultSink) -> Self {
        PhysicalStreamingResults { sink }
    }
}

impl BaseOperator for PhysicalStreamingResults {
    const OPERATOR_NAME: &str = "StreamingResults";

    type OperatorState = StreamingResultsOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(StreamingResultsOperatorState {
            sink: self.sink.clone(),
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
        //
        // TODO: Should probably error if the existing size is 0 since that'd
        // indicate we have multiple sinks for the same stream.
        let mut inner = operator_state.sink.inner.lock();
        inner.push_wakers.resize(partitions, None);
        inner.remaining_inputs = partitions;

        let states = (0..partitions)
            .map(|partition_idx| StreamingResultsPartitionState {
                finished: false,
                partition_idx,
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        // TODO: We should probably be filtering these out during the core
        // execution loop.
        if input.num_rows() == 0 {
            return Ok(PollPush::NeedsMore);
        }

        let mut inner = operator_state.sink.inner.lock();

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
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        debug_assert!(!state.finished);
        state.finished = true;

        let mut inner = operator_state.sink.inner.lock();
        inner.remaining_inputs -= 1;

        if let Some(waker) = inner.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalStreamingResults {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::operator::{CountingWaker, OperatorWrapper};
    use crate::util::task::noop_context;

    #[test]
    fn single_partition_stream() {
        let mut stream = ResultStream::new();
        let wrapper = OperatorWrapper::new(PhysicalStreamingResults::new(stream.sink()));

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();

        let mut input = generate_batch!([1, 2, 3, 4]);
        let poll = wrapper
            .poll_push(&op_state, &mut states[0], &mut input)
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
        let wrapper = OperatorWrapper::new(PhysicalStreamingResults::new(stream.sink()));

        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();
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
            .poll_push(&op_state, &mut states[0], &mut input)
            .unwrap();
        assert_eq!(PollPush::NeedsMore, poll);

        // Should have woken the stream.
        assert_eq!(1, stream_waker.wake_count());
    }
}
