use crate::arrow::chunk::{Chunk, TypeSchema};
use crate::arrow::row::Row;
use crate::errors::{LemurError, Result};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::{ChunkStream, MemoryStream, PinnedChunkStream, QueryExecutor};

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
    fn execute(self) -> Result<PinnedChunkStream> {
        let chunk = Chunk::empty_with_schema(self.schema);
        Ok(Box::pin(MemoryStream::new([chunk])))
    }
}
