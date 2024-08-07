use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use dashmap::DashMap;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};

use crate::{
    execution::{
        intermediate::StreamId,
        operators::{
            sink::{PartitionSink, QuerySink},
            source::{PartitionSource, QuerySource},
        },
    },
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};

use super::client::{IpcBatch, PullStatus};

/// Holds streams used for hybrid execution.
// TODO: Remove old streams.
#[derive(Debug, Default)]
pub struct ServerStreamBuffers {
    /// Streams that are sending batches to the server.
    incoming: DashMap<StreamId, IncomingStream>,
    /// Streams that are sending batches to the client.
    outgoing: DashMap<StreamId, OutgoingStream>,
}

impl ServerStreamBuffers {
    pub fn create_incoming_stream(&self, stream_id: StreamId) -> IncomingStream {
        let stream = IncomingStream {
            state: Arc::new(Mutex::new(IncomingStreamState {
                finished: false,
                batches: VecDeque::new(),
                pull_waker: None,
            })),
        };

        self.incoming.insert(stream_id, stream.clone());

        stream
    }

    pub fn create_outgoing_stream(&self, stream_id: StreamId) -> OutgoingStream {
        let stream = OutgoingStream {
            state: Arc::new(Mutex::new(OutgoingStreamState {
                finished: false,
                batch: None,
                push_waker: None,
            })),
        };

        self.outgoing.insert(stream_id, stream.clone());

        stream
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

impl QuerySink for OutgoingStream {
    fn create_partition_sinks(&self, num_sinks: usize) -> Vec<Box<dyn PartitionSink>> {
        assert_eq!(1, num_sinks);

        vec![Box::new(OutgoingPartitionStream {
            state: self.state.clone(),
        })]
    }

    fn partition_requirement(&self) -> Option<usize> {
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
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(OutgoingPushFuture {
            batch: Some(batch),
            state: self.state.clone(),
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(OutgoingFinalizeFuture {
            state: self.state.clone(),
        })
    }
}

#[derive(Debug)]
struct OutgoingStreamState {
    finished: bool,
    batch: Option<Batch>,
    push_waker: Option<Waker>,
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

impl QuerySource for IncomingStream {
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>> {
        assert_eq!(1, num_sources);

        vec![Box::new(IncomingPartitionStream {
            state: self.state.clone(),
        })]
    }

    fn partition_requirement(&self) -> Option<usize> {
        Some(1)
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
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(IncomingPullFuture {
            state: self.state.clone(),
        })
    }
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
