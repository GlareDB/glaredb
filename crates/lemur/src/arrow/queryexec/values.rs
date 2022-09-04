use crate::arrow::chunk::Chunk;
use crate::arrow::row::Row;
use crate::errors::{LemurError, Result};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::{ChunkStream, MemoryStream, PinnedChunkStream, QueryExecutor};

const DEFAULT_VALUES_CHUNK_SIZE: usize = 256;

#[derive(Debug, Clone)]
pub struct Values {
    rows: Vec<Row>,
    chunk_size: usize,
}

impl Values {
    pub fn new(rows: Vec<Row>) -> Values {
        Values {
            rows,
            chunk_size: DEFAULT_VALUES_CHUNK_SIZE,
        }
    }
}

impl QueryExecutor for Values {
    fn execute(self) -> Result<PinnedChunkStream> {
        let row_chunks: Vec<_> = self.rows.chunks(self.chunk_size).collect();
        let chunks = row_chunks
            .into_iter()
            .map(|rows| Chunk::from_rows(rows.to_vec()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(MemoryStream::new(chunks)))
    }
}
