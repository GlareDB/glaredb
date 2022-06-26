use crate::batch::{Batch, BatchRepr};
use crate::datatype::RelationSchema;
use crate::expr::ScalarExpr;
use anyhow::Result;
use futures::stream::{self, Stream};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<BatchRepr>> + Send>>;

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
    type Item = Result<BatchRepr>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.streamed {
            Poll::Ready(None)
        } else {
            self.streamed = true;
            Poll::Ready(Some(Ok(self.batch.clone().into())))
        }
    }
}
