//! Small crate for telemetry code.
use segment::message::{BatchMessage, Message, Track, User};
use segment::{Batcher, Client, HttpClient};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};
use uuid::Uuid;

const MESSAGE_BUF_SIZE: usize = 256;

pub enum Tracker {
    Segment(SegmentTracker),
    Nop,
}

impl Tracker {
    pub fn track<S: Into<String>>(&self, event: S, user_id: Uuid, properties: serde_json::Value) {
        match self {
            Tracker::Segment(t) => t.track(event, user_id, properties),
            Tracker::Nop => (), // Do nothing.
        }
    }
}

impl From<SegmentTracker> for Tracker {
    fn from(value: SegmentTracker) -> Self {
        Tracker::Segment(value)
    }
}

impl Default for Tracker {
    fn default() -> Self {
        Tracker::Nop
    }
}

pub struct SegmentTracker {
    tx: mpsc::Sender<BatchMessage>,
    /// Handle for background task responsible for sending segment events.
    _handle: JoinHandle<()>,
}

impl SegmentTracker {
    pub fn new(segment_key: String) -> SegmentTracker {
        let client = HttpClient::default();
        let (tx, rx) = mpsc::channel(MESSAGE_BUF_SIZE);

        let bulk = BulkBatcher {
            segment_key,
            client,
            rx,
        };

        let handle = tokio::spawn(async move { bulk.run().await });

        SegmentTracker {
            tx,
            _handle: handle,
        }
    }

    fn track<S: Into<String>>(&self, event: S, user_id: Uuid, properties: serde_json::Value) {
        let msg = BatchMessage::Track(Track {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            event: event.into(),
            properties,
            ..Default::default()
        });

        if let Err(e) = self.tx.try_send(msg) {
            error!(%e, "failed to send track message");
        }
    }
}

/// Send batches to Segment in bulk.
///
/// Differs from `AutoBatch` (provided by the `segment` crate) in that this will
/// read as many messages as possible, but does not wait until the batch's
/// buffer fills up before sending. This helps with ensuring that we are
/// consistently sending messages, and there's a lower possibility that we drop
/// messages on shutdown.
struct BulkBatcher {
    segment_key: String,
    client: HttpClient,
    rx: mpsc::Receiver<BatchMessage>,
}

impl BulkBatcher {
    async fn run(mut self) {
        loop {
            let mut batch = Batcher::new(None);

            // Get first message.
            match self.rx.recv().await {
                Some(msg) => {
                    batch = self.push(batch, msg).await;
                }
                None => {
                    debug!("channel closed for segment bulk batcher");
                    return;
                }
            }

            // Drain rest.
            //
            // Errors on empty or disconnected. Disconnected error is fine,
            // means we'll exit on next loop iteration.
            while let Ok(msg) = self.rx.try_recv() {
                batch = self.push(batch, msg).await;
            }

            // And flush.
            self.flush(batch).await
        }
    }

    async fn push(&self, mut batch: Batcher, msg: BatchMessage) -> Batcher {
        match batch.push(msg) {
            Ok(None) => batch,
            Ok(Some(msg)) => {
                self.flush(batch).await;
                let mut batch = Batcher::new(None);
                batch.push(msg).unwrap();
                batch
            }
            Err(e) => {
                error!(%e, "failed to push msg onto batch");
                batch
            }
        }
    }

    async fn flush(&self, batch: Batcher) {
        let msg = batch.into_message();
        if let Message::Batch(msg) = &msg {
            if msg.batch.is_empty() {
                // No need to send if empty batch.
                return;
            }
        }

        if let Err(e) = self.client.send(self.segment_key.clone(), msg).await {
            error!(%e, "failed to send batch to segment");
        }
    }
}
