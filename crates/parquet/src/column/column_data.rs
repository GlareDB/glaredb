use rayexec_error::Result;
use rayexec_execution::arrays::array::buffer_manager::BufferManager;
use rayexec_execution::arrays::array::raw::ByteBuffer;

use super::read_buffer::ReadBuffer;

#[derive(Debug)]
pub struct ColumnData<B: BufferManager> {
    pub(crate) chunk: ByteBuffer<B>,
    pub(crate) decompressed_page: ReadBuffer<B>,
}

impl<B> ColumnData<B>
where
    B: BufferManager,
{
    /// Create emtpy buffers for the column data.
    ///
    /// During page reading, these will be resized as appropriate.
    pub fn empty(manager: &B) -> Self {
        ColumnData {
            chunk: ByteBuffer::empty(manager),
            decompressed_page: ReadBuffer::empty(manager),
        }
    }

    /// Create a new buffer using the given chunk bytes.
    ///
    /// This is mostly useful for tests.
    pub fn with_chunk_bytes(manager: &B, bytes: impl AsRef<[u8]>) -> Result<Self> {
        let bytes = bytes.as_ref();
        let mut chunk = ByteBuffer::try_with_capacity(manager, bytes.len())?;

        let s = chunk.as_slice_mut();
        s.copy_from_slice(bytes);

        Ok(ColumnData {
            chunk,
            decompressed_page: ReadBuffer::empty(manager),
        })
    }
}
