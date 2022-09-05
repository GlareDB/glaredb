use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct Aggregate {}

impl QueryExecutor for Aggregate {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
