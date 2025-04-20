use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::buffer::db_vec::DbVec;
use glaredb_error::{DbError, Result};

/// Read buffer that owns the underlying buffer.
#[derive(Debug)]
pub struct OwnedReadBuffer {
    /// The underlying buffer.
    buffer: DbVec<u8>,
    /// Pointer to the current position in the buffer.
    curr: *const u8,
    /// Remaining number of bytes until the end of the buffer relative to the
    /// current pointer.
    remaining: usize,
}

unsafe impl Sync for OwnedReadBuffer {}
unsafe impl Send for OwnedReadBuffer {}

impl OwnedReadBuffer {
    /// Create a new read buffer from the given byte buffer.
    ///
    /// This buffer's `remaining` count will be initialized to the capacity of
    /// the byte buffer.
    pub fn new(buffer: DbVec<u8>) -> Self {
        let curr = buffer.as_ptr();
        let remaining = buffer.capacity();

        OwnedReadBuffer {
            buffer,
            curr,
            remaining,
        }
    }

    /// Try to create a new read buffer from the given bytes.
    ///
    /// This will allocated a byte buffer using the manager, which may have a
    /// capacity larger than what's required to fit `bs`. However the resulting
    /// buffer will have its `remaining` count set to the length of `bs`.
    ///
    /// Useful mostly for tests.
    #[allow(unused)]
    pub fn from_bytes(manager: &impl AsRawBufferManager, bs: impl AsRef<[u8]>) -> Result<Self> {
        let bs = bs.as_ref();
        let mut buffer = DbVec::<u8>::new_from_slice(manager, bs)?;

        let curr = buffer.as_ptr();
        let remaining = bs.len();

        Ok(OwnedReadBuffer {
            buffer,
            curr,
            remaining,
        })
    }

    /// Skip the next number of bytes.
    pub fn skip(&mut self, num_bytes: usize) -> Result<()> {
        if num_bytes > self.remaining {
            return Err(DbError::new("Attempted to skip more bytes than remaining")
                .with_field("remaining", self.remaining)
                .with_field("take", num_bytes));
        }

        // SAFETY: We check that this would be in bounds above.
        self.curr = unsafe { self.curr.byte_add(num_bytes) };
        self.remaining -= num_bytes;

        Ok(())
    }

    /// Gets a shared buffer for the next number of bytes.
    ///
    /// This will move the internal pointer forward in the buffer.
    pub fn take_next(&mut self, num_bytes: usize) -> Result<ReadCursor> {
        let shared = ReadCursor {
            curr: self.curr,
            remaining: num_bytes,
        };

        self.skip(num_bytes)?;

        Ok(shared)
    }

    /// Takes the remaining number of bytes.
    pub fn take_remaining(&mut self) -> ReadCursor {
        let shared = ReadCursor {
            curr: self.curr,
            remaining: self.remaining,
        };

        self.curr = unsafe { self.curr.byte_add(self.remaining) };
        self.remaining = 0;

        shared
    }

    /// Get the remaining bytes for this buffer.
    ///
    /// If called immediately after a reset, this will get the entire buffer.
    ///
    /// # Safety
    ///
    /// This must not be called with concurrent reads by any of the previously
    /// take read buffers.
    pub unsafe fn remaining_as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.curr.cast_mut(), self.remaining) }
    }

    /// Resets and resizes the underlying buffer to `size`.
    ///
    /// This may allocate a new buffer if the current buffer is not sufficient
    /// to hold the new size.
    ///
    /// This buffer's `remaining` count will be set to `size`. This may be less
    /// than the true capacity of the buffer as we may overallocate.
    ///
    /// # Safety
    ///
    /// All shared buffers created from this buffer are no longer valid to use.
    pub unsafe fn reset_and_resize(&mut self, size: usize) -> Result<()> {
        unsafe { self.buffer.resize_uninit(size)? };
        self.curr = self.buffer.as_ptr();
        debug_assert!(self.buffer.capacity() >= size);
        self.remaining = size;
        Ok(())
    }
}

/// Holds a pointer to some other buffer for reading.
///
/// This allows for multiple "readers" on top of a separate buffer.
///
/// Increments the pointer during reads. All reads are unaligned pointer reads
///
/// # Safety
///
/// Pretty unsafe. This requires that the original buffer this was created from
/// outlives this buffer.
///
/// All methods working with the pointer are marked as unsafe since we cannot
/// guarantee that the pointer is still valid.
#[derive(Debug)]
pub struct ReadCursor {
    /// Pointer to the current position in the buffer.
    curr: *const u8,
    /// Remaining number of bytes until the end of the buffer relative to the
    /// current pointer.
    remaining: usize,
}

unsafe impl Sync for ReadCursor {}
unsafe impl Send for ReadCursor {}

impl ReadCursor {
    /// Create a read cursor from a slice.
    ///
    /// # Safety
    ///
    /// The _creation_ of this read buffer is always safe, however to read
    /// values from it requires that the slice outlives this cursor.
    pub fn from_slice<T>(slice: &[T]) -> Self {
        let ptr = slice.as_ptr().cast();
        let remaining = std::mem::size_of_val(slice);

        ReadCursor {
            curr: ptr,
            remaining,
        }
    }

    /// Returns the remaining number of bytes in this cursor.
    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    /// Skips the pointer forward some number of bytes.
    pub unsafe fn skip_bytes_unchecked(&mut self, num_bytes: usize) {
        debug_assert!(self.remaining >= num_bytes);

        unsafe {
            self.curr = self.curr.byte_add(num_bytes);
            self.remaining -= num_bytes;
        }
    }

    /// Takes `num_bytes` from this cursor, returning a new cursor contain those
    /// bytes.
    ///
    /// Advances this cursor by `num_bytes` bytes.
    pub unsafe fn take(&mut self, num_bytes: usize) -> Self {
        debug_assert!(self.remaining >= num_bytes);

        let sliced = ReadCursor {
            curr: self.curr,
            remaining: num_bytes,
        };

        unsafe { self.skip_bytes_unchecked(num_bytes) };

        sliced
    }

    /// Reads the next value from the buffer, incrementing the internal pointer.
    ///
    /// # Safety
    ///
    /// This buffer must have enough bytes to read the next value fully.
    pub unsafe fn read_next_unchecked<T>(&mut self) -> T {
        debug_assert!(
            self.remaining >= std::mem::size_of::<T>(),
            "remaining: {}, need: {}",
            self.remaining,
            std::mem::size_of::<T>()
        );

        unsafe {
            let v = self.curr.cast::<T>().read_unaligned();
            self.skip_bytes_unchecked(std::mem::size_of::<T>());

            v
        }
    }

    /// Reads the next `len` number of bytes, returning the byte slice.
    ///
    /// # Safety
    ///
    /// `len` must not go beyond the end of the remaining number of bytes in the
    /// buffer.
    pub unsafe fn read_bytes_unchecked(&mut self, len: usize) -> &[u8] {
        debug_assert!(self.remaining >= len);

        unsafe {
            let bs = std::slice::from_raw_parts(self.curr, len);
            self.skip_bytes_unchecked(len);

            bs
        }
    }

    /// Copies bytes from this buffer into the output slice. The output slice
    /// must be less than or equal to the current remaining capacity of this
    /// buffer.
    ///
    /// This will increment the internal pointers.
    ///
    /// # Panics
    ///
    /// Panics if the output slice is larger than the remaining buffer.
    pub unsafe fn read_into_unchecked<T>(&mut self, out: &mut [T]) {
        unsafe {
            let byte_count = std::mem::size_of_val(out);
            assert!(byte_count <= self.remaining);

            let dest_ptr = out.as_mut_ptr().cast::<u8>();
            self.curr.copy_to_nonoverlapping(dest_ptr, byte_count);

            self.skip_bytes_unchecked(byte_count);
        }
    }

    /// Peeks the next value without incrementing the internal pointer.
    pub unsafe fn peek_next_unchecked<T>(&self) -> T {
        unsafe {
            debug_assert!(
                self.remaining >= std::mem::size_of::<T>(),
                "remaining: {}, need: {}",
                self.remaining,
                std::mem::size_of::<T>()
            );
            self.curr.cast::<T>().read_unaligned()
        }
    }
}

#[cfg(test)]
mod tests {
    use glaredb_core::buffer::buffer_manager::DefaultBufferManager;

    use super::*;

    #[test]
    fn take_all_read_u8() {
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, [0, 1, 2, 3]).unwrap();
        let mut s = buf.take_remaining();

        assert_eq!(0, unsafe { s.read_next_unchecked::<u8>() });
        assert_eq!(1, unsafe { s.read_next_unchecked::<u8>() });
        assert_eq!(2, unsafe { s.read_next_unchecked::<u8>() });
        assert_eq!(3, unsafe { s.read_next_unchecked::<u8>() });
    }

    #[test]
    fn skip_some_read_u8() {
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, [0, 1, 2, 3]).unwrap();
        buf.skip(2).unwrap();
        let mut s = buf.take_remaining();

        assert_eq!(2, unsafe { s.read_next_unchecked::<u8>() });
        assert_eq!(3, unsafe { s.read_next_unchecked::<u8>() });
    }

    #[test]
    fn take_some_read_u8() {
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, [0, 1, 2, 3]).unwrap();

        let mut s1 = buf.take_next(2).unwrap();
        assert_eq!(0, unsafe { s1.read_next_unchecked::<u8>() });
        assert_eq!(1, unsafe { s1.read_next_unchecked::<u8>() });

        let mut s2 = buf.take_next(2).unwrap();
        assert_eq!(2, unsafe { s2.read_next_unchecked::<u8>() });
        assert_eq!(3, unsafe { s2.read_next_unchecked::<u8>() });
    }

    #[test]
    fn take_all_read_u16() {
        // Assumes le
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, [0, 1, 2, 3]).unwrap();
        let mut s = buf.take_remaining();

        assert_eq!(256, unsafe { s.read_next_unchecked::<u16>() });
        assert_eq!(770, unsafe { s.read_next_unchecked::<u16>() });
    }

    #[test]
    fn take_all_read_into_u8() {
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, [0, 1, 2, 3]).unwrap();
        let mut s = buf.take_remaining();

        let mut out = [0; 3];
        unsafe {
            s.read_into_unchecked::<u8>(&mut out);
        }

        assert_eq!([0, 1, 2], out);
        assert_eq!(3, unsafe { s.read_next_unchecked::<u8>() });
    }

    #[test]
    fn take_all_peek_u16() {
        // Assumes le
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, [0, 1, 2, 3]).unwrap();
        let mut s = buf.take_remaining();

        assert_eq!(256, unsafe { s.peek_next_unchecked::<u16>() });
        assert_eq!(256, unsafe { s.read_next_unchecked::<u16>() });

        assert_eq!(770, unsafe { s.peek_next_unchecked::<u16>() });
        assert_eq!(770, unsafe { s.read_next_unchecked::<u16>() });
    }

    #[test]
    fn take_all_read_bytes() {
        let mut buf = OwnedReadBuffer::from_bytes(&DefaultBufferManager, b"hello").unwrap();
        let mut s = buf.take_remaining();

        let out = unsafe { s.read_bytes_unchecked(1) };
        assert_eq!(b"h", out);

        let out = unsafe { s.read_bytes_unchecked(3) };
        assert_eq!(b"ell", out);

        let out = unsafe { s.read_bytes_unchecked(1) };
        assert_eq!(b"o", out);
    }
}
