use coretypes::batch::{Batch, BatchRepr};
use coretypes::datatype::RelationSchema;
use coretypes::expr::ScalarExpr;
use futures::stream::{self, Stream};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error(transparent)]
    Forward(#[from] Box<dyn std::error::Error>),
}

pub type StreamResult<T, E = StreamError> = std::result::Result<T, E>;

pub type BatchStream = Pin<Box<dyn Stream<Item = StreamResult<BatchRepr>> + Send>>;

/// A stream of in-memory values.
#[derive(Debug)]
pub struct MemoryStream {
    batch: Batch,
    /// TODO: Split batch up into chunks and replace this with an index.
    streamed: bool,
}

impl MemoryStream {
    pub fn with_single_batch(batch: Batch) -> MemoryStream {
        MemoryStream {
            batch,
            streamed: false,
        }
    }
}

impl Stream for MemoryStream {
    type Item = StreamResult<BatchRepr>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.streamed {
            Poll::Ready(None)
        } else {
            self.streamed = true;
            Poll::Ready(Some(Ok(self.batch.clone().into())))
        }
    }
}
