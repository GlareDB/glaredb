use rayexec_error::Result;

use super::chunk::ColumnChunk;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::AsRawBufferManager;

#[derive(Debug)]
pub struct ColumnCollectionSegment {
    /// Capacity to use when creating new chunks.
    chunk_capacity: usize,
    /// All chunks in this segment.
    chunks: Vec<ColumnChunk>,
}

impl ColumnCollectionSegment {
    /// Creates a new collection segment with all chunks having the given chunk
    /// capacity.
    pub fn new(chunk_capacity: usize) -> Self {
        ColumnCollectionSegment {
            chunk_capacity,
            chunks: Vec::new(),
        }
    }

    /// Sets the relative offset for all chunks in this segment.
    pub fn set_relative_offsets(&mut self, relative_offset: usize) {
        let mut offset = relative_offset;

        for chunk in &mut self.chunks {
            chunk.relative_offset = offset;
            offset += chunk.filled;
        }
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Get the total number of rows for this segment.
    pub fn num_rows(&self) -> usize {
        self.chunks.iter().map(|c| c.filled).sum()
    }

    /// Appends a batch to this segment.
    // TODO: Bit weird having this accept datatypes.
    pub fn append_batch(
        &mut self,
        manager: &impl AsRawBufferManager,
        batch: &Batch,
        types: &[DataType],
    ) -> Result<()> {
        // Ensure we have at least one chunk to use.
        if self.chunks.is_empty() {
            let chunk = ColumnChunk::try_new(manager, types, self.chunk_capacity)?;
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

                let new_chunk = ColumnChunk::try_new(manager, types, self.chunk_capacity)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::generate_batch;

    #[test]
    fn append_fits_in_single_chunk() {
        let mut segment = ColumnCollectionSegment::new(4);

        let input = generate_batch!([1, 2, 3, 4]);
        segment
            .append_batch(&NopBufferManager, &input, &[DataType::Int32])
            .unwrap();

        assert_eq!(1, segment.num_chunks());
        assert_eq!(4, segment.get_chunk(0).unwrap().filled);
        assert_eq!(4, segment.get_chunk(0).unwrap().capacity);
    }

    #[test]
    fn append_requires_multiple_chunks() {
        let mut segment = ColumnCollectionSegment::new(4);

        let input = generate_batch!([1, 2, 3, 4, 5, 6]);
        segment
            .append_batch(&NopBufferManager, &input, &[DataType::Int32])
            .unwrap();

        assert_eq!(2, segment.num_chunks());

        assert_eq!(4, segment.get_chunk(0).unwrap().filled);
        assert_eq!(4, segment.get_chunk(0).unwrap().capacity);

        assert_eq!(2, segment.get_chunk(1).unwrap().filled);
        assert_eq!(4, segment.get_chunk(1).unwrap().capacity);
    }
}
