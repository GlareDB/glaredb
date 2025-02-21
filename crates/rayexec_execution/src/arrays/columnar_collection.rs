use std::fmt;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::array_buffer::ArrayBuffer;
use crate::arrays::array::validity::Validity;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::copy::copy_rows_raw;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::{AsRawBufferManager, NopBufferManager};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionState {
    Appending,
    Scanning,
}

impl fmt::Display for CollectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Appending => write!(f, "Appending"),
            Self::Scanning => write!(f, "Scanning"),
        }
    }
}

/// Collects array data and keeps the data in columnar format.
///
/// Internally this uses the same buffer types as arrays, but with the
/// assumption that everything is writable during the append phase.
///
/// When appending is complete, we flip to the Scanning phase. This will ensure
/// all buffers in the collection are using a shared reference.
#[derive(Debug)]
pub struct ColumnarCollection {
    /// Datatypes for the arrays being stored in this collection.
    types: Vec<DataType>,
    /// All chunks making up this collection.
    ///
    /// When a new chunk is append, the previous chunk will have all of its
    /// buffers made into shared references.
    chunks: Vec<ColumnarChunk>,
    /// Capacity to initialize each chunk with.
    chunk_capacity: usize,
    /// Current state of the collection.
    state: CollectionState,
}

impl ColumnarCollection {
    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(|c| c.filled).sum()
    }

    /// Appends a batch to this collection.
    pub fn append_batch(&mut self, batch: &Batch) -> Result<()> {
        self.check_state(CollectionState::Appending)?;

        // Ensure we have at least one chunk to use.
        if self.chunks.is_empty() {
            let chunk =
                ColumnarChunk::try_new(&NopBufferManager, &self.types, self.chunk_capacity)?;
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

                let new_chunk =
                    ColumnarChunk::try_new(&NopBufferManager, &self.types, self.chunk_capacity)?;
                self.chunks.push(new_chunk);
            }
        }

        Ok(())
    }

    /// Check that the collection state is in the expected state.
    fn check_state(&self, expected: CollectionState) -> Result<()> {
        if self.state != expected {
            return Err(RayexecError::new("Collection not in expected state")
                .with_field("current", self.state)
                .with_field("expected", expected));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ColumnarChunk {
    buffers: Vec<ColumnarBuffer>,
    capacity: usize,
    filled: usize,
}

impl ColumnarChunk {
    fn try_new(
        manager: &impl AsRawBufferManager,
        datatypes: &[DataType],
        capacity: usize,
    ) -> Result<Self> {
        let mut buffers = Vec::with_capacity(datatypes.len());
        for datatype in datatypes {
            let buffer = ArrayBuffer::try_new_for_datatype(manager, datatype, capacity)?;
            buffers.push(ColumnarBuffer {
                validity: Validity::new_all_valid(capacity),
                buffer,
            });
        }

        Ok(ColumnarChunk {
            buffers,
            capacity,
            filled: 0,
        })
    }

    /// Make all buffers in this chunk shared.
    fn make_all_shared(&mut self) {
        for buf in &mut self.buffers {
            buf.buffer.make_shared();
        }
    }

    /// Copy rows from the arrays into the chunk buffers.
    ///
    /// `src` must contain the same number of arrays of the buffers in this
    /// chunk.
    ///
    /// `count` must not exceed the remaining capacity of the chunk.
    fn copy_rows(&mut self, src: &[Array], src_offset: usize, count: usize) -> Result<()> {
        debug_assert_eq!(self.buffers.len(), src.len());
        debug_assert!(count <= self.capacity - self.filled);

        for (src, dest) in src.iter().zip(&mut self.buffers) {
            dest.copy_rows_from_array(self.filled, src, src_offset, count)?;
        }
        self.filled += count;

        Ok(())
    }
}

#[derive(Debug)]
struct ColumnarBuffer {
    validity: Validity,
    buffer: ArrayBuffer,
}

impl ColumnarBuffer {
    /// Copy rows from a source buffer and validity into this buffer.
    fn copy_rows_from_array(
        &mut self,
        dest_offset: usize,
        src: &Array,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        let phys_type = src.datatype().physical_type();

        // src => dest mapping.
        let mapping = (src_offset..(src_offset + count)).zip(dest_offset..(dest_offset + count));

        if src.should_flatten_for_execution() {
            let src = src.flatten()?;
            copy_rows_raw(
                phys_type,
                src.array_buffer,
                src.validity,
                Some(src.selection),
                mapping,
                &mut self.buffer,
                &mut self.validity,
            )
        } else {
            copy_rows_raw(
                phys_type,
                &src.data,
                &src.validity,
                None,
                mapping,
                &mut self.buffer,
                &mut self.validity,
            )
        }
    }
}
