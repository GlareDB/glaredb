
use rayexec_error::Result;

use super::hash_table::HashTable;
use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

/// Drains a hash table.
///
/// The output will be ordered by [RESULT, GROUPS].
#[derive(Debug)]
pub struct HashTableDrain {
    /// The table we're draining from.
    pub(crate) table: HashTable,
    /// The current chunk we're draining.
    pub(crate) drain_idx: usize,
    /// Batch size to use when draining.
    pub(crate) batch_size: usize,
}

impl HashTableDrain {
    fn next_inner(&mut self) -> Result<Option<Batch>> {
        if self.drain_idx >= self.table.chunks.len() {
            return Ok(None);
        }

        let chunk = &mut self.table.chunks[self.drain_idx];

        let mut arrays = self
            .table
            .aggregates
            .iter()
            .map(|agg| {
                Array::try_new(
                    &NopBufferManager,
                    agg.datatype.clone(),
                    self.batch_size,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let mut count = 0;

        for (out, agg_state) in arrays.iter_mut().zip(&mut chunk.aggregate_states) {
            // Assumes all agg states drain the number of value, which they
            // should.
            count = agg_state.states.drain(out)?;
        }

        if count < self.batch_size {
            // Only move to next chunk once this one is drained.
            self.drain_idx += 1;
        }

        let mut batch = Batch::try_from_arrays(arrays)?;
        batch.set_num_rows(count)?;

        Ok(Some(batch))
    }
}

impl Iterator for HashTableDrain {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
