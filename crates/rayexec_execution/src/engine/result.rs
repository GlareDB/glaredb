use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use parking_lot::Mutex;
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::{RayexecError, Result};

use crate::{
    execution::{operators::PollPush, query_graph::sink::PartitionSink},
    runtime::{ErrorSink, QueryHandle},
};

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: Schema,
    pub stream: ResultAdapterStream,
    pub handle: Box<dyn QueryHandle>,
}

#[derive(Debug)]
pub struct ResultAdapterStream {
    state: Arc<Mutex<AdapterState>>,
    batch_sink_count: usize,
}

#[derive(Debug)]
struct AdapterState {
    /// If we've ever errored, the stream is done.
    did_error: bool,

    /// If push side is finished.
    finished: bool,

    /// Buffered error.
    ///
    /// Currently this just stores the first error encountered.
    error: Option<RayexecError>,

    /// Buffered batch.
    batch: Option<Batch>,

    /// Push side waker.
    push_waker: Option<Waker>,

    /// Pull side waker.
    pull_waker: Option<Waker>,
}

impl ResultAdapterStream {
    pub fn new() -> Self {
        ResultAdapterStream {
            state: Arc::new(Mutex::new(AdapterState {
                did_error: false,
                finished: false,
                error: None,
                batch: None,
                push_waker: None,
                pull_waker: None,
            })),
            batch_sink_count: 0,
        }
    }

    pub fn error_sink(&self) -> AdapterErrorSink {
        AdapterErrorSink {
            state: self.state.clone(),
        }
    }

    pub fn partition_sink(&mut self) -> ResultAdapterSink {
        assert_eq!(
            0, self.batch_sink_count,
            "only one query sink should exist for adapter stream"
        );
        self.batch_sink_count += 1;
        ResultAdapterSink {
            state: self.state.clone(),
        }
    }
}

impl Default for ResultAdapterStream {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream for ResultAdapterStream {
    type Item = Result<Batch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock();

        if let Some(error) = state.error.take() {
            return Poll::Ready(Some(Err(error)));
        }

        if state.did_error {
            return Poll::Ready(None);
        }

        let poll = match state.batch.take() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => {
                if state.finished {
                    return Poll::Ready(None);
                }

                state.pull_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        };

        if let Some(waker) = state.push_waker.take() {
            waker.wake();
        }

        poll
    }
}

#[derive(Debug)]
pub struct ResultAdapterSink {
    state: Arc<Mutex<AdapterState>>,
}

impl PartitionSink for ResultAdapterSink {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        let mut state = self.state.lock();

        if state.batch.is_some() {
            state.push_waker = Some(cx.waker().clone());
            return Ok(PollPush::Pending(batch));
        }

        state.batch = Some(batch);
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPush::Pushed)
    }

    fn finalize_push(&mut self) -> Result<()> {
        let mut state = self.state.lock();
        state.finished = true;
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AdapterErrorSink {
    state: Arc<Mutex<AdapterState>>,
}

impl ErrorSink for AdapterErrorSink {
    fn push_error(&self, error: RayexecError) {
        let mut state = self.state.lock();
        state.did_error = true;
        if state.error.is_none() {
            state.error = Some(error);
        }
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }
    }
}
