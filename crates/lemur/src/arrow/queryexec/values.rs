use crate::arrow::chunk::Chunk;
use crate::arrow::row::Row;
use crate::errors::Result;

use super::{MemoryStream, PinnedChunkStream, QueryExecutor};

const DEFAULT_VALUES_CHUNK_SIZE: usize = 256;

#[derive(Debug, Clone)]
pub struct ChunkValues {
    chunks: Vec<Chunk>,
}

impl ChunkValues {
    pub fn from_chunk(chunk: Chunk) -> ChunkValues {
        ChunkValues {
            chunks: vec![chunk],
        }
    }

    pub fn from_chunks(chunks: Vec<Chunk>) -> ChunkValues {
        ChunkValues { chunks }
    }
}

impl QueryExecutor for ChunkValues {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        Ok(Box::pin(MemoryStream::new(self.chunks)))
    }
}

impl From<ChunkValues> for Box<dyn QueryExecutor> {
    fn from(v: ChunkValues) -> Self {
        Box::new(v)
    }
}

#[derive(Debug, Clone)]
pub struct RowValues {
    rows: Vec<Row>,
    chunk_size: usize,
}

impl RowValues {
    pub fn new(rows: Vec<Row>) -> RowValues {
        RowValues {
            rows,
            chunk_size: DEFAULT_VALUES_CHUNK_SIZE,
        }
    }
}

impl QueryExecutor for RowValues {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        let chunks = self
            .rows
            .chunks(self.chunk_size)
            .map(|rows| Chunk::from_rows(rows.to_vec()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(MemoryStream::new(chunks)))
    }
}

impl From<RowValues> for Box<dyn QueryExecutor> {
    fn from(v: RowValues) -> Self {
        Box::new(v)
    }
}
