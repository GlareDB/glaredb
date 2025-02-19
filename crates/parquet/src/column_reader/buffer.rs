use rayexec_execution::arrays::array::buffer_manager::BufferManager;
use rayexec_execution::arrays::array::raw::TypedRawBuffer;

/// Reusable and resizable byte buffer.
#[derive(Debug)]
pub struct ReadBuffer<B: BufferManager> {
    /// The byte read offset into the buffer.
    offset: usize,
    buffer: TypedRawBuffer<u8, B>,
}

impl<B> ReadBuffer<B>
where
    B: BufferManager,
{
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
}
