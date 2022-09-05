use crate::arrow::chunk::Chunk;
use crate::arrow::expr::ScalarExpr;
use crate::arrow::row::Row;
use crate::errors::{LemurError, Result};
use futures::stream::{self, Stream, StreamExt};

use super::{MemoryStream, PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct SortMergeJoin {}

impl QueryExecutor for SortMergeJoin {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
