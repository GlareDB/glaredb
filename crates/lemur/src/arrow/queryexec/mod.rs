//! Executors for queries from the database.

use crate::arrow::chunk::Chunk;
use crate::errors::Result;
use futures::Stream;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

mod aggregate;
mod crossjoin;
mod filter;
mod hashjoin;
mod index;
mod mergejoin;
mod placeholder;
mod project;
mod table;
mod values;

pub use aggregate::*;
pub use crossjoin::*;
pub use filter::*;
pub use hashjoin::*;
pub use index::*;
pub use mergejoin::*;
pub use placeholder::*;
pub use project::*;
pub use table::*;
pub use values::*;

pub type PinnedChunkStream = Pin<Box<dyn Stream<Item = Result<Chunk>> + Send>>;

pub trait QueryExecutor: Debug + Sync + Send {
    /// Execute self against a chunk stream.
    ///
    /// Note that this takes a boxed self.
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream>;
}

/// Stream an in-memory set of chunks. This will never produce an error on the
/// stream.
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.chunks.pop_front() {
            Some(chunk) => Poll::Ready(Some(Ok(chunk))),
            None => Poll::Ready(None),
        }
    }
}
