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
enum PendingStream<S> {
    StreamArrivedFirst(S),
    WaitingForStream(oneshot::Sender<S>),
}

pub type StagedClientStreams = StagedStreams<ClientExchangeRecvStream>;

/// Hold streams that pending for execution.
#[derive(Clone)]
pub struct StagedStreams<S> {
    /// Streams keyed by broadcast id.
    streams: Arc<Mutex<HashMap<Uuid, PendingStream<S>>>>,
}

impl<S> StagedStreams<S> {
    /// Put a stream for later execution.
    pub fn put_stream(&self, id: Uuid, stream: S) {
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
    pub async fn resolve_pending_stream(&self, id: Uuid) -> Result<S> {
        // Happens in a block to properly scope the lock guard to ensure this
        // struct is `Send`.
        //
        // Can early return if we happen to already have the stream.
        let rx = {
            let mut streams = self.streams.lock();

            // Handle case where we already have the stream.
            if let Some(pending) = streams.remove(&id) {
                match pending {
                    PendingStream::StreamArrivedFirst(stream) => return Ok(stream),
                    PendingStream::WaitingForStream(_) => {
                        // Programmer bug.
                        panic!("attempted to resolve stream twice")
                    }
                }
            }

            // Handle case where we need to wait for the stream to arrive.
            let (tx, rx) = oneshot::channel();
            streams.insert(id, PendingStream::WaitingForStream(tx));

            rx
        };

        let stream = rx.await.map_err(|_| internal!("stream sender dropped"))?;

        Ok(stream)
    }
}

impl<S> Default for StagedStreams<S> {
    fn default() -> Self {
        StagedStreams {
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_puts_stream_first() {
        let staged = StagedStreams::<usize>::default();
        let id = Uuid::new_v4();
        staged.put_stream(id, 8);

        let out = staged.resolve_pending_stream(id).await.unwrap();
        assert_eq!(8, out);
    }

    #[tokio::test]
    async fn server_executes_first() {
        let staged = StagedStreams::<usize>::default();
        let id = Uuid::new_v4();

        let cloned = staged.clone();
        // Try to resolve first.
        let handle = tokio::spawn(async move { cloned.resolve_pending_stream(id).await.unwrap() });

        // Put in some other "streams" that isn't the one we want.
        staged.put_stream(Uuid::new_v4(), 1);
        staged.put_stream(Uuid::new_v4(), 2);
        staged.put_stream(Uuid::new_v4(), 3);

        // Put in our client "stream".
        staged.put_stream(id, 8);

        // We should now get our "stream" back.
        let out = handle.await.unwrap();

        assert_eq!(8, out);
    }
}
