use crate::execution::operators::PollPush;
use crate::execution::query_graph::sink::PartitionSink;
use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Create an unpartitioned result stream.
pub fn unpartitioned_result_stream() -> (ResultStream, Box<dyn PartitionSink>) {
    let (sender, recv) = mpsc::channel(1);
    let sink = ResultPartitionSink { sender };

    let (errors_send, errors_recv) = std::sync::mpsc::channel();

    (
        ResultStream {
            recv,
            errors_send,
            errors_recv,
            did_error: false,
        },
        Box::new(sink),
    )
}

#[derive(Debug)]
pub struct ResultStream {
    errors_send: std::sync::mpsc::Sender<RayexecError>,
    errors_recv: std::sync::mpsc::Receiver<RayexecError>,

    /// If we've received an error, the stream should be considered completed.
    /// This lets us return the appropropriate Poll.
    did_error: bool,

    recv: mpsc::Receiver<Batch>,
}

impl ResultStream {
    pub fn errors_send_channel(&self) -> std::sync::mpsc::Sender<RayexecError> {
        self.errors_send.clone()
    }
}

impl Stream for ResultStream {
    type Item = Result<Batch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.did_error {
            return Poll::Ready(None);
        }

        if let Ok(e) = self.errors_recv.try_recv() {
            self.did_error = true;
            return Poll::Ready(Some(Err(e)));
        }

        match self.recv.poll_next_unpin(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct ResultPartitionSink {
    sender: mpsc::Sender<Batch>,
}

impl PartitionSink for ResultPartitionSink {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                match self.sender.start_send(batch) {
                    Ok(_) => Ok(PollPush::Pushed),
                    Err(_) => Ok(PollPush::Break), // TODO: What to do, receiving end disconnected between poll ready and start send.
                }
            }
            Poll::Ready(Err(e)) => {
                if e.is_full() {
                    Ok(PollPush::Pending(batch))
                } else {
                    // TODO: What to do? Receiving end closed.
                    Ok(PollPush::Break)
                }
            }
            Poll::Pending => Ok(PollPush::Pending(batch)),
        }
    }

    fn finalize_push(&mut self) -> Result<()> {
        self.sender.close_channel();
        Ok(())
    }
}
