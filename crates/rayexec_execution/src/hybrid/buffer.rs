use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use dashmap::DashMap;
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use rayexec_error::{RayexecError, Result};
use tracing::debug;
use uuid::Uuid;

use super::client::{IpcBatch, PullStatus};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::intermediate::pipeline::StreamId;
use crate::execution::operators::sink::operation::{PartitionSink, PollPush, SinkOperation};
use crate::execution::operators::source::operation::{PartitionSource, PollPull, SourceOperation};
use crate::execution::operators::PollFinalize;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::runtime::ErrorSink;

/// Holds streams used for hybrid execution.
// TODO: Remove old streams.
#[derive(Debug, Default)]
pub struct ServerStreamBuffers {
    /// Error sinks keyed by query id.
    error_sinks: DashMap<Uuid, Arc<SharedErrorSink>>,
    /// Streams that are sending batches to the server.
    incoming: DashMap<StreamId, IncomingStream>,
    /// Streams that are sending batches to the client.
    outgoing: DashMap<StreamId, OutgoingStream>,
}

impl ServerStreamBuffers {
    pub fn create_incoming_stream(&self, stream_id: StreamId) -> Result<IncomingStream> {
        debug!(?stream_id, "creating incoming stream");

        let stream = IncomingStream {
            state: Arc::new(Mutex::new(IncomingStreamState {
                finished: false,
                batches: VecDeque::new(),
                pull_waker: None,
            })),
        };

        self.incoming.insert(stream_id, stream.clone());

        Ok(stream)
    }

    pub fn create_outgoing_stream(&self, stream_id: StreamId) -> Result<OutgoingStream> {
        debug!(?stream_id, "creating outgoing stream");

        let error_sink = self.get_sink_for_query(&stream_id.query_id)?;

        let stream = OutgoingStream {
            state: Arc::new(Mutex::new(OutgoingStreamState {
                finished: false,
                batch: None,
                push_waker: None,
                error_sink,
            })),
        };

        self.outgoing.insert(stream_id, stream.clone());

        Ok(stream)
    }

    /// Creates an stores an error sink for a query.
    pub fn create_error_sink(&self, query_id: Uuid) -> Result<Arc<SharedErrorSink>> {
        debug!(%query_id, "create error sink for query");

        match self.error_sinks.entry(query_id) {
            dashmap::Entry::Occupied(_ent) => Err(RayexecError::new(format!(
                "Error sink already exists for query {query_id}"
            ))),
            dashmap::Entry::Vacant(ent) => {
                let error_sink = Arc::new(SharedErrorSink::default());
                ent.insert(error_sink.clone());
                Ok(error_sink)
            }
        }
    }

    pub fn get_sink_for_query(&self, query_id: &Uuid) -> Result<Arc<SharedErrorSink>> {
        debug!(%query_id, "retrieving error sink for query");

        let error_sink = self
            .error_sinks
            .get(query_id)
            .ok_or_else(|| RayexecError::new(format!("Missing error sink for query {query_id}")))?;

        Ok(error_sink.value().clone())
    }

    pub fn push_batch_for_stream(&self, stream_id: &StreamId, batch: Batch) -> Result<()> {
        let incoming = self.incoming.get(stream_id).ok_or_else(|| {
            RayexecError::new(format!("Missing incoming stream with id: {stream_id:?}"))
        })?;

        let mut state = incoming.state.lock();
        state.batches.push_back(batch);

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(())
    }

    pub fn finalize_stream(&self, stream_id: &StreamId) -> Result<()> {
        let incoming = self.incoming.get(stream_id).ok_or_else(|| {
            RayexecError::new(format!("Missing incoming stream with id: {stream_id:?}"))
        })?;

        let mut state = incoming.state.lock();
        state.finished = true;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(())
    }

    pub fn pull_batch_for_stream(&self, stream_id: &StreamId) -> Result<PullStatus> {
        let outgoing = self.outgoing.get(stream_id).ok_or_else(|| {
            RayexecError::new(format!("Missing outgoing stream with id: {stream_id:?}"))
        })?;

        let mut state = outgoing.state.lock();

        // Check if the query errored before doing anything with the batch. This
        // is how we get the error back to the client.
        //
        // This may race between the read/write of the lock, but I don't think
        // that actually matters. The error is getting back to the client
        // somehow, either via this stream or some other stream.
        if state.error_sink.inner.read().is_some() {
            let error = state.error_sink.inner.write().take();
            return Err(error.unwrap_or_else(|| RayexecError::new("Error already pulled")));
        }

        let status = match state.batch.take() {
            Some(batch) => PullStatus::Batch(IpcBatch(batch)),
            None if state.finished => PullStatus::Finished,
            None => PullStatus::Pending,
        };

        if let Some(waker) = state.push_waker.take() {
            waker.wake();
        }

        Ok(status)
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingStream {
    state: Arc<Mutex<OutgoingStreamState>>,
}

impl SinkOperation for OutgoingStream {
    fn create_partition_sinks(
        &mut self,
        _context: &DatabaseContext,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        assert_eq!(1, partitions);

        Ok(vec![Box::new(OutgoingPartitionStream {
            state: self.state.clone(),
        })])
    }

    fn partitioning_requirement(&self) -> Option<usize> {
        Some(1)
    }
}

impl Explainable for OutgoingStream {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("OutgoingStream")
    }
}

#[derive(Debug)]
pub struct OutgoingPartitionStream {
    state: Arc<Mutex<OutgoingStreamState>>,
}

impl PartitionSink for OutgoingPartitionStream {
    fn poll_push(&mut self, cx: &mut Context, input: &mut Batch) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize> {
        unimplemented!()
    }

    // fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
    //     Box::pin(OutgoingPushFuture {
    //         batch: Some(batch),
    //         state: self.state.clone(),
    //     })
    // }

    // fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
    //     Box::pin(OutgoingFinalizeFuture {
    //         state: self.state.clone(),
    //     })
    // }
}

#[derive(Debug)]
struct OutgoingStreamState {
    finished: bool,
    batch: Option<Batch>,
    push_waker: Option<Waker>,
    error_sink: Arc<SharedErrorSink>,
}

struct OutgoingPushFuture {
    batch: Option<Batch>,
    state: Arc<Mutex<OutgoingStreamState>>,
}

impl Future for OutgoingPushFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut state = this.state.lock();

        if state.batch.is_some() {
            state.push_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        state.batch = this.batch.take();

        Poll::Ready(Ok(()))
    }
}

struct OutgoingFinalizeFuture {
    state: Arc<Mutex<OutgoingStreamState>>,
}

impl Future for OutgoingFinalizeFuture {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock();
        state.finished = true;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Clone)]
pub struct IncomingStream {
    state: Arc<Mutex<IncomingStreamState>>,
}

impl SourceOperation for IncomingStream {
    fn create_partition_sources(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        unimplemented!()
        // assert_eq!(1, num_sources);

        // vec![Box::new(IncomingPartitionStream {
        //     state: self.state.clone(),
        // })]
    }
}

impl Explainable for IncomingStream {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("IncomingStream")
    }
}

#[derive(Debug)]
pub struct IncomingPartitionStream {
    state: Arc<Mutex<IncomingStreamState>>,
}

impl PartitionSource for IncomingPartitionStream {
    fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        unimplemented!()
    }

    // fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
    //     Box::pin(IncomingPullFuture {
    //         state: self.state.clone(),
    //     })
    // }
}

#[derive(Debug)]
struct IncomingStreamState {
    finished: bool,
    batches: VecDeque<Batch>,
    pull_waker: Option<Waker>,
}

struct IncomingPullFuture {
    state: Arc<Mutex<IncomingStreamState>>,
}

impl Future for IncomingPullFuture {
    type Output = Result<Option<Batch>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock();

        match state.batches.pop_front() {
            Some(batch) => Poll::Ready(Ok(Some(batch))),
            None if state.finished => Poll::Ready(Ok(None)),
            None => {
                state.pull_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Error sink shared across all stream buffers for a particular query.
#[derive(Debug, Default)]
pub struct SharedErrorSink {
    inner: RwLock<Option<RayexecError>>,
}

impl ErrorSink for SharedErrorSink {
    fn push_error(&self, error: RayexecError) {
        let mut inner = self.inner.write();
        if inner.is_some() {
            // Prefer the existing error.
            return;
        }

        *inner = Some(error);
    }
}
