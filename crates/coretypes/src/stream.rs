use crate::batch::{Batch, BatchRepr};
use anyhow::{anyhow, Result};
use futures::sink::Sink;
use futures::stream::{Stream, StreamExt};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<BatchRepr>> + Send>>;

/// A stream created from a list of in-memory batches.
#[derive(Debug)]
pub struct MemoryStream {
    batches: VecDeque<BatchRepr>,
}

impl MemoryStream {
    pub fn empty() -> MemoryStream {
        MemoryStream {
            batches: VecDeque::new(),
        }
    }

    pub fn from_batch<B>(batch: B) -> MemoryStream
    where
        B: Into<BatchRepr>,
    {
        MemoryStream {
            batches: VecDeque::from([batch.into()]),
        }
    }

    pub fn from_batches<I, B>(batches: I) -> MemoryStream
    where
        B: Into<BatchRepr>,
        I: IntoIterator<Item = B>,
    {
        MemoryStream {
            batches: batches.into_iter().map(|b| b.into()).collect(),
        }
    }

    fn current_size(&self) -> usize {
        self.batches.len()
    }
}

impl Stream for MemoryStream {
    type Item = Result<BatchRepr>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Ready(None),
        }
    }
}

/// A type indicating which side of a either stream the batch was returned
/// from.
pub enum EitherBatch {
    Left(Result<BatchRepr>),
    Right(Result<BatchRepr>),
}

impl EitherBatch {
    pub fn is_left(&self) -> bool {
        matches!(self, EitherBatch::Left(_))
    }

    pub fn is_right(&self) -> bool {
        matches!(self, EitherBatch::Right(_))
    }

    pub fn into_result(self) -> Result<BatchRepr> {
        match self {
            EitherBatch::Left(r) => r,
            EitherBatch::Right(r) => r,
        }
    }
}

/// Combine two batch streams into a single stream.
///
/// Items returned from the stream are wrapped in an `EitherBatch` to indicate
/// which stream the item came from.
///
/// Order is not guaranteed.
pub struct EitherStream {
    left: BatchStream,
    right: BatchStream,
}

impl EitherStream {
    pub fn from_streams(left: BatchStream, right: BatchStream) -> EitherStream {
        EitherStream { left, right }
    }
}

impl Stream for EitherStream {
    type Item = EitherBatch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.left.poll_next_unpin(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(EitherBatch::Left(batch))),
            Poll::Ready(None) => match self.right.poll_next_unpin(cx) {
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(batch)) => Poll::Ready(Some(EitherBatch::Right(batch))),
                Poll::Pending => Poll::Pending,
            },
            Poll::Pending => match self.right.poll_next_unpin(cx) {
                Poll::Ready(Some(batch)) => Poll::Ready(Some(EitherBatch::Right(batch))),
                Poll::Ready(None) | Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// An in-memory sink for holding batches.
///
/// Useful when a batch needs to be iterated over multiple times (e.g. during
/// joins). Batches can be sent to the sink, then streamed back out.
#[derive(Debug)]
pub struct MemorySink {
    batches: VecDeque<BatchRepr>,
    terminated: bool,
}

impl MemorySink {
    pub fn new() -> MemorySink {
        MemorySink {
            batches: VecDeque::new(),
            terminated: false,
        }
    }
}

impl<B: Into<BatchRepr>> Sink<B> for MemorySink {
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.terminated {
            return Poll::Ready(Err(anyhow!("sink terminated")));
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: B) -> Result<(), Self::Error> {
        if self.terminated {
            return Err(anyhow!("sink terminated"));
        }
        self.batches.push_back(item.into());
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.terminated {
            return Poll::Ready(Err(anyhow!("sink terminated")));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.terminated = true;
        Poll::Ready(Ok(()))
    }
}

impl Stream for MemorySink {
    type Item = Result<BatchRepr>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        match self.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn either_stream() {
        // (left, right)
        let test_cases = vec![
            (
                MemoryStream::from_batches([Batch::empty(), Batch::empty(), Batch::empty()]),
                MemoryStream::from_batches([Batch::empty(), Batch::empty()]),
            ),
            (
                MemoryStream::from_batches([Batch::empty(), Batch::empty()]),
                MemoryStream::from_batches([Batch::empty(), Batch::empty(), Batch::empty()]),
            ),
            (
                MemoryStream::empty(),
                MemoryStream::from_batches([Batch::empty(), Batch::empty()]),
            ),
            (
                MemoryStream::from_batches([Batch::empty(), Batch::empty()]),
                MemoryStream::empty(),
            ),
        ];

        for (left, right) in test_cases.into_iter() {
            let expected_left = left.current_size();
            let expected_right = right.current_size();

            let stream = EitherStream::from_streams(Box::pin(left), Box::pin(right));
            let items = stream.collect::<Vec<_>>().await;

            let left_count = items
                .iter()
                .fold(0, |acc, item| if item.is_left() { acc + 1 } else { acc });
            assert_eq!(expected_left, left_count);
            let right_count = items
                .iter()
                .fold(0, |acc, item| if item.is_right() { acc + 1 } else { acc });
            assert_eq!(expected_right, right_count);
        }
    }
}
