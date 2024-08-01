use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures::{future::BoxFuture, Future, Stream};
use parking_lot::Mutex;
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::{RayexecError, Result};
use tracing::warn;

use crate::{
    execution::operators::sink::{PartitionSink, QuerySink},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    runtime::{ErrorSink, QueryHandle},
};

/// Create sinks and streams for sending query output to a client.
pub fn new_results_sinks() -> (ResultStream, ResultSink, ResultErrorSink) {
    let inner = Arc::new(Mutex::new(InnerState {
        batch: None,
        error: None,
        finished: false,
        push_waker: None,
        pull_waker: None,
    }));

    (
        ResultStream {
            inner: inner.clone(),
        },
        ResultSink {
            inner: inner.clone(),
        },
        ResultErrorSink { inner },
    )
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: Schema,
    pub stream: ResultStream,
    pub handle: Box<dyn QueryHandle>,
}

#[derive(Debug)]
pub struct ResultStream {
    inner: Arc<Mutex<InnerState>>,
}

impl Stream for ResultStream {
    type Item = Result<Batch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();

        if let Some(error) = inner.error.take() {
            return Poll::Ready(Some(Err(error)));
        }

        if let Some(batch) = inner.batch.take() {
            return Poll::Ready(Some(Ok(batch)));
        }

        if inner.finished {
            return Poll::Ready(None);
        }

        inner.pull_waker = Some(cx.waker().clone());

        if let Some(push_waker) = inner.push_waker.take() {
            push_waker.wake();
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub struct ResultSink {
    inner: Arc<Mutex<InnerState>>,
}

impl QuerySink for ResultSink {
    fn create_partition_sinks(&self, num_sinks: usize) -> Vec<Box<dyn PartitionSink>> {
        (0..num_sinks)
            .map(|_| {
                Box::new(ResultPartitionSink {
                    inner: self.inner.clone(),
                }) as _
            })
            .collect()
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(1)
    }
}

impl Explainable for ResultSink {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ResultSink")
    }
}

#[derive(Debug)]
pub struct ResultPartitionSink {
    inner: Arc<Mutex<InnerState>>,
}

impl PartitionSink for ResultPartitionSink {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(PushFuture {
            batch: Some(batch),
            inner: self.inner.clone(),
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(FinalizeFuture {
            inner: self.inner.clone(),
        })
    }
}

#[derive(Debug)]
pub struct ResultErrorSink {
    inner: Arc<Mutex<InnerState>>,
}

impl ErrorSink for ResultErrorSink {
    fn push_error(&self, error: RayexecError) {
        warn!(%error, "query error");

        // First error wins.
        let mut inner = self.inner.lock();
        if inner.error.is_none() {
            inner.error = Some(error);
        }

        if let Some(waker) = inner.pull_waker.take() {
            waker.wake();
        }
    }
}

/// State shared between the result stream/sink and error sink.
///
/// This lets us inject an error into the stream that arises outside of stream.
#[derive(Debug)]
struct InnerState {
    batch: Option<Batch>,
    error: Option<RayexecError>,
    finished: bool,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

struct PushFuture {
    batch: Option<Batch>,
    inner: Arc<Mutex<InnerState>>,
}

impl Future for PushFuture {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut inner = this.inner.lock();
        if inner.batch.is_some() {
            inner.push_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        inner.batch = this.batch.take();

        if let Some(pull_waker) = inner.pull_waker.take() {
            pull_waker.wake();
        }

        Poll::Ready(Ok(()))
    }
}

struct FinalizeFuture {
    inner: Arc<Mutex<InnerState>>,
}

impl Future for FinalizeFuture {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock();
        inner.finished = true;

        if let Some(pull_waker) = inner.pull_waker.take() {
            pull_waker.wake();
        }

        Poll::Ready(Ok(()))
    }
}
