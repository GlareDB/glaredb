use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use super::hash_table::HashTable;

/// Drains a hash table.
///
/// The output will be ordered by [RESULT, GROUPS].
#[derive(Debug)]
pub struct HashTableDrain {
    /// The table we're draining from.
    pub(crate) table: HashTable,
    /// The current chunk we're draining.
    pub(crate) drain_idx: usize,
}

impl HashTableDrain {
    fn next_inner(&mut self) -> Result<Option<Batch>> {
        if self.drain_idx >= self.table.chunks.len() {
            return Ok(None);
        }

        let chunk = &mut self.table.chunks[self.drain_idx];
        self.drain_idx += 1;

        // Computed aggregate columns.
        let results = chunk
            .aggregate_states
            .iter_mut()
            .map(|s| s.states.drain())
            .collect::<Result<Vec<_>>>()?;

        // Chunk arrays includes the GROUP ID column (last).
        let batch = Batch::try_new(results.into_iter().chain(chunk.arrays.drain(..)))?;

        Ok(Some(batch))
    }
}

impl Iterator for HashTableDrain {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
