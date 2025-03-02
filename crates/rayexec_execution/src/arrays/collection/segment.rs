use rayexec_error::Result;

use super::chunk::ColumnChunk;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::AsRawBufferManager;

#[derive(Debug)]
pub struct ColumnCollectionSegment {
    chunks: Vec<ColumnChunk>,
}

impl ColumnCollectionSegment {
    pub fn new() -> Self {
        ColumnCollectionSegment { chunks: Vec::new() }
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    pub fn append_batch(
        &mut self,
        manager: &impl AsRawBufferManager,
        batch: &Batch,
        types: &[DataType],
        chunk_capacity: usize,
    ) -> Result<()> {
        // Ensure we have at least one chunk to use.
        if self.chunks.is_empty() {
            let chunk = ColumnChunk::try_new(manager, types, chunk_capacity)?;
            self.chunks.push(chunk);
        }

        let mut input_offset = 0; // Offset to begin copying from in the input.
        let mut rows_remaining = batch.num_rows();

        while rows_remaining != 0 {
            let chunk = self.chunks.last_mut().expect("at least one chunk");
            let copy_count = usize::min(chunk.capacity - chunk.filled, rows_remaining);

            chunk.copy_rows(&batch.arrays, input_offset, copy_count)?;
            input_offset += copy_count;
            rows_remaining -= copy_count;

            if rows_remaining > 0 {
                // Didn't fit everything into this chunk. Turn all buffers in
                // the current chunk into shared buffers, and create a new chunk
                // for writing to.
                chunk.make_all_shared();

                let new_chunk = ColumnChunk::try_new(manager, types, chunk_capacity)?;
                self.chunks.push(new_chunk);
            }
        }

        Ok(())
    }

    pub fn finish_append(&mut self) {
        // Only need to make the last shared, all other chunks should have
        // already be inside arcs.
        if let Some(last) = self.chunks.last_mut() {
            last.make_all_shared();
        }
    }

    pub fn get_chunk(&self, chunk_idx: usize) -> Option<&ColumnChunk> {
        self.chunks.get(chunk_idx)
    }
}
