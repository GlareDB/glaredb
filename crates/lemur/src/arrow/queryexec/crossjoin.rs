use crate::arrow::chunk::Chunk;
use crate::arrow::row::Row;
use crate::errors::{LemurError, Result};
use futures::stream::{self, Stream, StreamExt};

use super::{MemoryStream, PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct CrossJoin {
    left: Box<dyn QueryExecutor>,
    right: Box<dyn QueryExecutor>,
}

impl QueryExecutor for CrossJoin {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
