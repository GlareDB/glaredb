use crate::arrow::chunk::{Chunk, TypeSchema};
use crate::errors::Result;

use super::{MemoryStream, PinnedChunkStream, QueryExecutor};

#[derive(Debug, Clone)]
pub struct Placeholder {
    schema: TypeSchema,
}

impl Placeholder {
    pub fn new(schema: TypeSchema) -> Placeholder {
        Placeholder { schema }
    }
}

impl QueryExecutor for Placeholder {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        let chunk = Chunk::empty_with_schema(self.schema);
        Ok(Box::pin(MemoryStream::new([chunk])))
    }
}
