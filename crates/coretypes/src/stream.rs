use crate::batch::{Batch, BatchRepr};
use anyhow::Result;
use futures::stream::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<BatchRepr>> + Send>>;

/// A stream created from a list of in-memory batches.
#[derive(Debug)]
pub struct MemoryStream {
    batches: VecDeque<Batch>,
}

impl MemoryStream {
    pub fn from_batch(batch: Batch) -> MemoryStream {
        MemoryStream {
            batches: VecDeque::from([batch]),
        }
    }

    pub fn from_batches<I>(batches: I) -> MemoryStream
    where
        I: IntoIterator<Item = Batch>,
    {
        MemoryStream {
            batches: batches.into_iter().collect(),
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<BatchRepr>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch.into()))),
            None => Poll::Ready(None),
        }
    }
}
