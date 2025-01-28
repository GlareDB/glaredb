use rayexec_error::Result;

use super::row_heap::RowHeap;
use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::raw::TypedRawBuffer;

/// Stores row encoded data.
#[derive(Debug)]
pub struct RowCollection {
    /// Layout for the rows.
    layout: RowLayout,
}

#[derive(Debug)]
struct RowChunk<B: BufferManager> {
    /// The raw buffer holding encoded row values.
    data: TypedRawBuffer<u8, B>,
    /// Number of rows this chunk can hold.
    capacity: usize,
    /// Number of rows we've written to this chunk.
    filled: usize,
    /// Heap for varlen and nested data.
    heap: RowHeap<B>,
}

impl<B> RowChunk<B>
where
    B: BufferManager,
{
    fn try_new(manager: &B, layout: &RowLayout, capacity: usize) -> Result<Self> {
        let byte_capacity = capacity * layout.row_width;

        let data = TypedRawBuffer::try_with_capacity(manager, byte_capacity)?;
        let heap = RowHeap::with_capacity(manager, 0)?;

        Ok(RowChunk {
            data,
            capacity,
            filled: 0,
            heap,
        })
    }
}
