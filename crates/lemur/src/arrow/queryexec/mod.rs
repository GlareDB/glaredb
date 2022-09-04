use crate::arrow::chunk::Chunk;
use crate::errors::{LemurError, Result};
use futures::Stream;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

mod values;
pub use values::*;
mod placeholder;
pub use placeholder::*;

/// Types that stream dataframes.
pub trait ChunkStream: Stream<Item = Result<Chunk>> {}

pub type PinnedChunkStream = Pin<Box<dyn ChunkStream + Send>>;

pub trait QueryExecutor: Debug + Sync + Send {
    /// Return an execution stream.
    fn execute(self) -> Result<PinnedChunkStream>;
}

pub struct MemoryStream {
    chunks: VecDeque<Chunk>,
}

impl MemoryStream {
    pub fn new(chunks: impl IntoIterator<Item = Chunk>) -> MemoryStream {
        MemoryStream {
            chunks: chunks.into_iter().collect(),
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.chunks.pop_front() {
            Some(chunk) => Poll::Ready(Some(Ok(chunk))),
            None => Poll::Ready(None),
        }
    }
}

impl ChunkStream for MemoryStream {}
