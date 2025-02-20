use rayexec_error::Result;
use rayexec_execution::arrays::array::buffer_manager::BufferManager;
use rayexec_execution::arrays::array::raw::ByteBuffer;

/// Reusable and resizable byte buffer.
// TODO: This could just store a pointer to a buffer. Specifically for
// uncompressed pages, we could just have a "view" in the chunk buffer.
#[derive(Debug)]
pub struct ReadBuffer<B: BufferManager> {
    /// The byte read offset into the buffer.
    offset: usize,
    buffer: ByteBuffer<B>,
}

impl<B> ReadBuffer<B>
where
    B: BufferManager,
{
    pub fn empty(manager: &B) -> Self {
        ReadBuffer {
            offset: 0,
            buffer: ByteBuffer::empty(manager),
        }
    }

    /// Try to create a new read buffer from the given bytes.
    ///
    /// Useful mostly for tests.
    pub fn from_bytes(manager: &B, bs: impl AsRef<[u8]>) -> Result<Self> {
        let bs = bs.as_ref();
        let mut buffer = ByteBuffer::try_with_capacity(manager, bs.len())?;

        buffer.as_slice_mut().copy_from_slice(bs);

        Ok(ReadBuffer { offset: 0, buffer })
    }

    pub fn reset_for_new_page(&mut self, page_size: usize) -> Result<()> {
        self.offset = 0;
        self.buffer.reserve_for_size(page_size)
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        // TODO: Should offset apply here?
        self.buffer.as_slice_mut()
    }

    pub fn increment_byte_offset(&mut self, count_bytes: usize) {
        debug_assert!(count_bytes + self.offset <= self.buffer.capacity());
        self.offset += count_bytes;
    }

    /// Copies bytes from this buffer into the output slice.
    ///
    /// This will internally increment the read offset.
    pub unsafe fn read_copy<T>(&mut self, out: &mut [T]) {
        let byte_count = out.len() * std::mem::size_of::<T>();
        assert!(byte_count + self.offset <= self.buffer.capacity());

        let dest_ptr = out.as_mut_ptr().cast::<u8>();
        let src_ptr = self.buffer.as_mut_ptr().byte_add(self.offset);

        src_ptr.copy_to_nonoverlapping(dest_ptr, byte_count);

        self.offset += byte_count;
    }

    /// Reads the next value from the buffer, incrementing the internal read
    /// offset.
    pub unsafe fn read_next<T>(&mut self) -> T {
        debug_assert!(self.offset + std::mem::size_of::<T>() <= self.buffer.capacity());

        let v = self
            .buffer
            .as_ptr()
            .byte_add(self.offset)
            .cast::<T>()
            .read_unaligned();
        self.offset += std::mem::size_of::<T>();

        v
    }

    /// Read the next value from the buffer without incrementing the internal
    /// read offset.
    pub unsafe fn peek_next<T>(&self) -> T {
        debug_assert!(self.offset + std::mem::size_of::<T>() <= self.buffer.capacity());

        self.buffer
            .as_ptr()
            .byte_add(self.offset)
            .cast::<T>()
            .read_unaligned()
    }
}
