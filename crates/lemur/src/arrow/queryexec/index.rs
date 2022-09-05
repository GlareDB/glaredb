use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct IndexScan {}

impl QueryExecutor for IndexScan {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
