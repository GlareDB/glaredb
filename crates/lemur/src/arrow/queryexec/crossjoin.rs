use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

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
