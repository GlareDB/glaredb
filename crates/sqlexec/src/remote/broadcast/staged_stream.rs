use std::collections::HashMap;
use std::sync::Arc;

use crate::errors::{internal, Result};
use parking_lot::Mutex;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::exchange_exec::ClientExchangeRecvStream;

/// State of the pending stream.
///
/// Since we need to coordinate between client and server, we have two states:
/// - Client begins streaming first.
/// - Server begins executing first.
///
/// Both states are correct since we have to "scheduler" triggering which
/// happens first. And so while the client will always send the plan to the
/// server first, the server may actually begin executing that plan prior to the
/// client actually starting to stream.
enum PendingStream {
    StreamArrivedFirst(ClientExchangeRecvStream),
    WaitingForStream(oneshot::Sender<ClientExchangeRecvStream>),
}

/// Hold streams that pending for execution.
#[derive(Clone, Default)]
pub struct StagedClientStreams {
    /// Streams keyed by broadcast id.
    streams: Arc<Mutex<HashMap<Uuid, PendingStream>>>,
}

impl StagedClientStreams {
    /// Put a stream for later execution.
    pub fn put_stream(&self, id: Uuid, stream: ClientExchangeRecvStream) {
        let mut streams = self.streams.lock();

        // Handle case where we began executing before the stream arrived.
        if let Some(pending) = streams.remove(&id) {
            match pending {
                PendingStream::StreamArrivedFirst(_) => panic!("attempted to put stream twice"), // Programmer bug.
                PendingStream::WaitingForStream(channel) => {
                    // We don't care if the receiver dropped. Means it was
                    // canceled on the "execution" side.
                    let _ = channel.send(stream);
                    return;
                }
            }
        }

        // Handle case where stream arrived first.
        streams.insert(id, PendingStream::StreamArrivedFirst(stream));
    }

    /// Resolve a pending stream by id.
    ///
    /// This will wait until we have the stream available.
    pub async fn resolve_pending_stream(&self, id: Uuid) -> Result<ClientExchangeRecvStream> {
        let mut streams = self.streams.lock();

        // Handle case where we already have the stream.
        if let Some(pending) = streams.remove(&id) {
            match pending {
                PendingStream::StreamArrivedFirst(stream) => return Ok(stream),
                PendingStream::WaitingForStream(_) => panic!("attempted to resolve stream twice"), // Programmer bug.
            }
        }

        // Handle case where we need to wait for the stream to arrive.
        let (tx, rx) = oneshot::channel();
        streams.insert(id, PendingStream::WaitingForStream(tx));

        // Avoid deadlock.
        std::mem::drop(streams);

        let stream = rx.await.map_err(|_| internal!("stream sender dropped"))?;

        Ok(stream)
    }
}
