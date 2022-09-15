use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct HashJoin {}

impl QueryExecutor for HashJoin {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
