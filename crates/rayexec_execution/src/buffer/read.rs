use rayexec_error::Result;

use super::buffer_manager::AsRawBufferManager;
use super::typed::ByteBuffer;

/// A resizable buffer that internally track its read position.
///
/// All reads are unaligned pointer reads.
#[derive(Debug)]
pub struct ReadBuffer {
    /// The underlying buffer.
    buffer: ByteBuffer,
    /// Pointer to the current position in the buffer.
    curr: *const u8,
    /// Remaining number of bytes until the end of the buffer relative to the
    /// current pointer.
    remaining: usize,
}

impl ReadBuffer {
    pub fn empty(manager: &impl AsRawBufferManager) -> Self {
        let buffer = ByteBuffer::empty(manager);
        let ptr = buffer.as_ptr();
        ReadBuffer {
            buffer,
            curr: ptr,
            remaining: 0,
        }
    }

    /// Try to create a new read buffer from the given bytes.
    ///
    /// Useful mostly for tests.
    pub fn from_bytes(manager: &impl AsRawBufferManager, bs: impl AsRef<[u8]>) -> Result<Self> {
        let bs = bs.as_ref();
        let mut buffer = ByteBuffer::try_with_capacity(manager, bs.len())?;

        buffer.as_slice_mut().copy_from_slice(bs);
        let curr = buffer.as_ptr();
        let remaining = buffer.capacity();

        Ok(ReadBuffer {
            buffer,
            curr,
            remaining,
        })
    }

    /// Resets the buffer to start reading from the beginning.
    pub fn reset(&mut self) {
        self.curr = self.buffer.as_ptr();
        self.remaining = self.buffer.capacity();
    }

    /// Attempts re-allocate to fit `new_size` bytes. May allocate more space
    /// than requested.
    ///
    /// If the current capacity can already hold `new_size` bytes, then no
    /// allocation is done.
    ///
    /// This will retain the pointer position within the buffer.
    pub fn reserve_for_size(&mut self, new_size: usize) -> Result<()> {
        // Get bytes we need to skip at the beginning of the buffer if we do end
        // up reallocating.
        let to_skip = self.buffer.capacity() - self.remaining;

        if self.buffer.reserve_for_size(new_size)? {
            // Need to update remaining, and recompute the pointer.
            self.remaining = self.buffer.capacity();
            self.curr = self.buffer.as_ptr();

            // Move the pointer to the right spot.
            unsafe {
                self.skip_unchecked(to_skip);
            }
        }

        Ok(())
    }

    pub fn remaining_as_slice(&self) -> &[u8] {
        // SAFETY: This can only be unsafe if the _unchecked methods cause the
        // pointer to go beyond the end of the buffer.
        unsafe { std::slice::from_raw_parts(self.curr, self.remaining) }
    }

    pub fn remaining_as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.curr.cast_mut(), self.remaining) }
    }

    /// Skips the pointer forward some number of bytes.
    pub unsafe fn skip_unchecked(&mut self, num_bytes: usize) {
        debug_assert!(self.remaining >= num_bytes);
        self.curr = self.curr.byte_add(num_bytes);
        self.remaining -= num_bytes;
    }

    /// Reads the next value in the buffer, returning None if the buffer doesn't
    /// have sufficient byte remaining.
    pub fn read_next<T>(&mut self) -> Option<T> {
        if self.remaining < std::mem::size_of::<T>() {
            return None;
        }

        let v = unsafe { self.read_next_unchecked::<T>() };
        Some(v)
    }

    /// Reads the next value from the buffer, incrementing the internal pointer.
    ///
    /// # Safety
    ///
    /// This buffer must have enough bytes to read the next value fully.
    pub unsafe fn read_next_unchecked<T>(&mut self) -> T {
        debug_assert!(self.remaining >= std::mem::size_of::<T>());

        let v = self.curr.cast::<T>().read_unaligned();
        self.skip_unchecked(std::mem::size_of::<T>());

        v
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
    pub fn read_copy<T>(&mut self, out: &mut [T]) {
        let byte_count = out.len() * std::mem::size_of::<T>();
        assert!(byte_count <= self.remaining);

        let dest_ptr = out.as_mut_ptr().cast::<u8>();
        unsafe {
            self.curr.copy_to_nonoverlapping(dest_ptr, byte_count);
        }

        unsafe {
            self.skip_unchecked(byte_count);
        }
    }

    pub fn peek_next<T>(&self) -> Option<T> {
        if self.remaining < std::mem::size_of::<T>() {
            return None;
        }

        let v = unsafe { self.peek_next_unchecked::<T>() };
        Some(v)
    }

    /// Peeks the next value without incrementing the internal pointer.
    pub unsafe fn peek_next_unchecked<T>(&self) -> T {
        debug_assert!(self.remaining >= std::mem::size_of::<T>());
        self.curr.cast::<T>().read_unaligned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;

    #[test]
    fn read_u8() {
        let mut buf = ReadBuffer::from_bytes(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        assert_eq!(Some(0), buf.read_next::<u8>());
        assert_eq!(Some(1), buf.read_next::<u8>());
        assert_eq!(Some(2), buf.read_next::<u8>());
        assert_eq!(Some(3), buf.read_next::<u8>());
        assert_eq!(None, buf.read_next::<u8>());
    }

    #[test]
    fn read_reset_u8() {
        let mut buf = ReadBuffer::from_bytes(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        assert_eq!(Some(0), buf.read_next::<u8>());
        assert_eq!(Some(1), buf.read_next::<u8>());
        buf.reset();
        assert_eq!(Some(0), buf.read_next::<u8>());
        assert_eq!(Some(1), buf.read_next::<u8>());
    }

    #[test]
    fn read_u16() {
        // Assumes le
        let mut buf = ReadBuffer::from_bytes(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        assert_eq!(Some(256), buf.read_next::<u16>());
        assert_eq!(Some(770), buf.read_next::<u16>());
        assert_eq!(None, buf.read_next::<u16>());
    }

    #[test]
    fn read_copy_u8() {
        let mut buf = ReadBuffer::from_bytes(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        let mut out = [0; 3];
        buf.read_copy::<u8>(&mut out);

        assert_eq!([0, 1, 2], out);
        assert_eq!(Some(3), buf.read_next::<u8>());
    }

    #[test]
    fn peek_u16() {
        // Assumes le
        let mut buf = ReadBuffer::from_bytes(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        assert_eq!(Some(256), buf.peek_next::<u16>());
        assert_eq!(Some(256), buf.read_next::<u16>());

        assert_eq!(Some(770), buf.peek_next::<u16>());
        assert_eq!(Some(770), buf.read_next::<u16>());
    }

    #[test]
    fn reserve_for_size_needs_reallocate() {
        let mut buf = ReadBuffer::from_bytes(&NopBufferManager, [0, 1, 2, 3]).unwrap();

        // Move pointer forward one.
        assert_eq!(Some(256), buf.read_next::<u16>());

        // Technically no guarantees around the initial size of the buffer...
        buf.reserve_for_size(2048).unwrap();

        // Ensure we retain the pointer position.
        assert_eq!(Some(770), buf.read_next::<u16>());

        // Ensure we have at least the remaining space available to us.
        let rem = buf.remaining_as_slice();
        assert!(rem.len() >= 2044, "len: {}", rem.len());
    }
}
