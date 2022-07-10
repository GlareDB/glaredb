use anyhow::{anyhow, Result};
use coretypes::batch::Batch;
use futures::Sink;
use futures::Stream;
use parking_lot::RwLock;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

// TODO: Spill to disk.
#[derive(Debug, Clone)]
pub struct BatchSpill {
    inner: Arc<RwLock<SpillInner>>,
}

#[derive(Debug)]
struct SpillInner {
    batches: Vec<Batch>,
    terminated: bool,
}

impl BatchSpill {
    pub fn new() -> BatchSpill {
        BatchSpill {
            inner: Arc::new(RwLock::new(SpillInner {
                batches: Vec::new(),
                terminated: false,
            })),
        }
    }

    pub fn num_spilled(&self) -> usize {
        let inner = self.inner.read();
        inner.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.num_spilled() == 0
    }

    /// Stream batches that have been spilled, starting at some index.
    pub fn stream_spilled(&self, start: usize) -> Result<SpillStream> {
        let len = {
            let inner = self.inner.read();
            inner.batches.len()
        };
        if start >= len {
            return Err(anyhow!("start index out of bounds"));
        }
        Ok(SpillStream {
            spill: self.clone(),
            curr: start,
            end: len - 1,
        })
    }
}

impl Sink<Batch> for BatchSpill {
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Batch) -> Result<(), Self::Error> {
        let mut inner = self.inner.write();
        inner.batches.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Stream spilled batches.
pub struct SpillStream {
    spill: BatchSpill,
    curr: usize,
    end: usize,
}

impl<'a> Stream for SpillStream {
    type Item = Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.curr > self.end {
            return Poll::Ready(None);
        }
        let batch = {
            let inner = self.spill.inner.read();
            inner.batches[self.curr].clone()
        };
        self.curr += 1;
        Poll::Ready(Some(batch))
    }
}
