use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct SortMergeJoin {}

impl QueryExecutor for SortMergeJoin {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
