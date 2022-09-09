use crate::arrow::chunk::Chunk;
use crate::arrow::column::{hash, Column};
use crate::errors::{internal, Result};
use futures::StreamExt;
use std::collections::HashMap;
use tracing::trace;

use super::{PinnedChunkStream, QueryExecutor};

// TODO: Move this elsewhere.
// TODO: Anti, semi
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug)]
pub struct HashJoin {
    left: Box<dyn QueryExecutor>,
    right: Box<dyn QueryExecutor>,
    join_type: JoinType,
    /// Column indexes to join on.
    ///
    /// Each tuple contains a relative column index from the left table, and a
    /// relative column indexe from the right table.
    on: Vec<(usize, usize)>,
}

impl HashJoin {}

impl QueryExecutor for HashJoin {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}

impl From<HashJoin> for Box<dyn QueryExecutor> {
    fn from(v: HashJoin) -> Self {
        Box::new(v)
    }
}

struct HashChunks {
    /// Map from a hash to tuples containing the relative index of a chunk, and
    /// a relative index of a row in that chunk.
    hashes: HashMap<u64, Vec<(usize, usize)>>, // TODO: Use raw.

    /// Chunks we've already probed.
    // TODO: We'll eventually want to spill out to disk as necessary.
    chunks: Vec<Chunk>,
}

impl HashChunks {
    /// Build a hash table from the provided input.
    ///
    /// `cols` indicates the column indices we'll be hashing on.
    async fn build(input: Box<dyn QueryExecutor>, cols: &[usize]) -> Result<HashChunks> {
        trace!(?cols, "building probe side");

        let mut hashes: HashMap<u64, Vec<(usize, usize)>> = HashMap::new();
        let mut chunks = Vec::new();

        let mut stream = input.execute_boxed()?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;

            let hash_cols = cols
                .iter()
                .map(|idx| chunk.get_column(*idx).cloned())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| internal!("missing column in chunk"))?;

            let col_hashes = hash::hash_columns(&hash_cols)?;

            let chunk_idx = chunks.len();
            for (row_idx, hash) in col_hashes.iter().enumerate() {
                if let Some(idxs) = hashes.get_mut(hash) {
                    idxs.push((chunk_idx, row_idx));
                } else {
                    hashes.insert(*hash, vec![(chunk_idx, row_idx)]);
                }
            }

            chunks.push(chunk);
        }

        Ok(HashChunks { hashes, chunks })
    }

    /// Probe the hashed chunks and build a resulting chunk from the input.
    fn probe(&self, input: &Chunk, cols: &[usize]) -> Result<Chunk> {
        unimplemented!()
    }
}
