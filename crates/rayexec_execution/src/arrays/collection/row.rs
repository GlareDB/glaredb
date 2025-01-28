use rayexec_error::Result;

use super::row_layout::RowLayout;
use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::raw::TypedRawBuffer;
use crate::arrays::array::string_view::StringViewHeap;

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
    /// Heap for string values.
    // TODO: More generic heap?
    //
    // Need a way to store lists.
    //
    // Also this will intermingle utf8 and binary data.
    heap: StringViewHeap,
}

impl<B> RowChunk<B>
where
    B: BufferManager,
{
    fn try_new(manager: &B, layout: &RowLayout, capacity: usize) -> Result<Self> {
        let byte_capacity = capacity * layout.row_width;

        let data = TypedRawBuffer::try_with_capacity(manager, byte_capacity)?;
        let heap = StringViewHeap::new(false);

        Ok(RowChunk {
            data,
            capacity,
            filled: 0,
            heap,
        })
    }
}
