use crate::execution::operators::PollPush;
use crate::execution::query_graph::sink::PartitionSink;
use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Create an unpartition result stream.
pub fn unpartitioned_result_stream() -> (ResultStream, Box<dyn PartitionSink>) {
    let (sender, recv) = mpsc::channel(1);
    let sink = ResultPartitionSink { sender };
    (ResultStream { recv }, Box::new(sink))
}

#[derive(Debug)]
pub struct ResultStream {
    recv: mpsc::Receiver<Batch>,
}

impl Stream for ResultStream {
    type Item = Batch;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.recv.poll_next_unpin(cx)
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
