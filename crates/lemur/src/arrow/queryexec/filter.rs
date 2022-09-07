use crate::arrow::expr::ScalarExpr;
use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct Filter {
    input: Box<dyn QueryExecutor>,
    predicate: ScalarExpr,
}

impl QueryExecutor for Filter {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}
