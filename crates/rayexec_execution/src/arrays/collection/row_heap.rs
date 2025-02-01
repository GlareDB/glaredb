use std::fmt;

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::raw::TypedRawBuffer;

/// Metadata for small heap data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RowHeapSmallMetadata {
    pub len: i32,
    pub inline: [u8; 12],
}

/// Metadata for large heap data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RowHeapLargeMetadata {
    pub len: i32,
    pub prefix: [u8; 4],
    pub buffer_idx: i32,
    pub offset: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowHeapMetadata<'a> {
    Small(&'a RowHeapSmallMetadata),
    Large(&'a RowHeapLargeMetadata),
}

#[derive(Clone, Copy)]
#[repr(C)]
pub union RowHeapMetadataUnion {
    small: RowHeapSmallMetadata,
    large: RowHeapLargeMetadata,
}

impl Default for RowHeapMetadataUnion {
    #[inline]
    fn default() -> Self {
        Self::zero()
    }
}

impl RowHeapMetadataUnion {
    const _SIZE_ASSERTION: () = assert!(std::mem::size_of::<Self>() == 16);

    #[inline]
    pub fn as_metadata(&self) -> RowHeapMetadata {
        unsafe {
            // i32 len is first field in both, safe to access from either
            // variant.
            if self.is_small() {
                RowHeapMetadata::Small(&self.small)
            } else {
                RowHeapMetadata::Large(&self.large)
            }
        }
    }

    pub const fn is_small(&self) -> bool {
        // SAFETYE: i32 len is first field in both, safe to access from either
        // variant.
        unsafe { self.small.len <= 12 }
    }

    pub const fn is_large(&self) -> bool {
        unsafe { self.small.len > 12 }
    }

    pub fn data_len(&self) -> i32 {
        // SAFETY: `len` field is in the same place in both variants.
        unsafe { self.small.len }
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        // SAFETY: Const assertion guarantees this is 16 bytes.
        unsafe { std::mem::transmute::<Self, [u8; 16]>(self) }
    }

    pub const fn from_bytes(buf: [u8; 16]) -> Self {
        // SAFETY: Const assersion guarantees this is 16 bytes.
        //
        // Note that this doesn't guarantee validity of the data, just that
        // these bytes can always be represented as the union.
        unsafe { std::mem::transmute::<[u8; 16], Self>(buf) }
    }

    pub(crate) const fn zero() -> Self {
        Self {
            small: RowHeapSmallMetadata {
                len: 0,
                inline: [0; 12],
            },
        }
    }

    fn as_small(&self) -> RowHeapSmallMetadata {
        debug_assert!(self.is_small());
        unsafe { self.small }
    }

    fn as_large(&self) -> RowHeapLargeMetadata {
        debug_assert!(!self.is_small());
        unsafe { self.large }
    }
}

impl From<RowHeapSmallMetadata> for RowHeapMetadataUnion {
    fn from(value: RowHeapSmallMetadata) -> Self {
        RowHeapMetadataUnion { small: value }
    }
}

impl From<RowHeapLargeMetadata> for RowHeapMetadataUnion {
    fn from(value: RowHeapLargeMetadata) -> Self {
        RowHeapMetadataUnion { large: value }
    }
}

impl fmt::Debug for RowHeapMetadataUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_small() {
            let small = self.as_small();
            small.fmt(f)
        } else {
            let large = self.as_large();
            large.fmt(f)
        }
    }
}

#[derive(Debug)]
pub struct RowHeap<B: BufferManager> {
    manager: B,
    buffers: Vec<RowHeapBuffer<B>>,
}

impl<B> RowHeap<B>
where
    B: BufferManager,
{
    pub const MAX_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
    pub const DEFAULT_BUFFER_SIZE: usize = 1024;

    /// Create a new heap with an initial capacity.
    pub fn with_capacity(manager: &B, mut capacity: usize) -> Result<Self> {
        let mut buffers = Vec::with_capacity(1);

        while capacity > 0 {
            let buf_cap = usize::min(capacity, Self::MAX_BUFFER_SIZE);
            let buffer = RowHeapBuffer::with_capacity(manager, buf_cap)?;
            buffers.push(buffer);
            capacity -= buf_cap;
        }

        Ok(RowHeap {
            manager: manager.clone(),
            buffers,
        })
    }

    pub fn clear(&mut self) {
        self.buffers.iter_mut().for_each(|b| b.filled = 0);
    }

    /// Push bytes into the heap.
    ///
    /// If the slice is small enough, it'll be inlined in the metadata.
    /// Otherwise the slice will be copied to the heap, and the returned
    /// metadata will point to right place in the buffer.
    pub fn push_bytes(&mut self, value: &[u8]) -> Result<RowHeapMetadataUnion> {
        if value.len() <= 12 {
            // Store completely inline.
            let mut inline = [0; 12];
            inline[0..value.len()].copy_from_slice(value);

            Ok(RowHeapSmallMetadata {
                len: value.len() as i32,
                inline,
            }
            .into())
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.

            let (buf_idx, buffer) = self.get_or_allocate_buffer(value.len())?;

            let offset = buffer.filled;

            // Copy prefix to metadata.
            let mut prefix = [0; 4];
            let prefix_len = std::cmp::min(value.len(), 4);
            prefix[0..prefix_len].copy_from_slice(&value[0..prefix_len]);

            // Copy entire value to buffer.
            let buf = &mut buffer.buffer.as_mut()[offset..(offset + value.len())];
            buf.copy_from_slice(value);

            buffer.filled += value.len();

            Ok(RowHeapLargeMetadata {
                len: value.len() as i32,
                prefix,
                buffer_idx: buf_idx as i32,
                offset: offset as i32,
            }
            .into())
        }
    }

    fn get_or_allocate_buffer(&mut self, len: usize) -> Result<(usize, &mut RowHeapBuffer<B>)> {
        if self.buffers.is_empty() {
            let buf = RowHeapBuffer::with_capacity(&self.manager, Self::DEFAULT_BUFFER_SIZE)?;
            self.buffers.push(buf);
        }

        let last_idx = self.buffers.len() - 1;

        if self.buffers[last_idx].remaining_capacity() >= len {
            // We have enough capacity.
            return Ok((last_idx, &mut self.buffers[last_idx]));
        }

        // Check if we can resize.
        if self.buffers[last_idx].buffer.capacity() + len <= Self::MAX_BUFFER_SIZE {
            // We can, go ahead and resize and return it.
            self.buffers[last_idx].reserve(len)?;
            return Ok((last_idx, &mut self.buffers[last_idx]));
        }

        let new_buf = RowHeapBuffer::with_capacity(&self.manager, Self::DEFAULT_BUFFER_SIZE)?;
        self.buffers.push(new_buf);

        // Return the new buffer instead
        Ok((self.buffers.len() - 1, self.buffers.last_mut().unwrap()))
    }

    pub fn get<'a, 'b: 'a>(&'b self, metadata: &'a RowHeapMetadataUnion) -> Option<&'a [u8]> {
        if metadata.is_small() {
            unsafe { Some(&metadata.small.inline[..(metadata.small.len as usize)]) }
        } else {
            unsafe {
                let offset = metadata.large.offset as usize;
                let len = metadata.large.len as usize;

                self.buffers[metadata.large.buffer_idx as usize]
                    .buffer
                    .as_slice()
                    .get(offset..(offset + len))
            }
        }
    }

    pub fn get_mut<'a, 'b: 'a>(
        &'b mut self,
        metadata: &'a mut RowHeapMetadataUnion,
    ) -> Option<&'a mut [u8]> {
        if metadata.is_small() {
            unsafe { Some(&mut metadata.small.inline[..(metadata.small.len as usize)]) }
        } else {
            unsafe {
                let offset = metadata.large.offset as usize;
                let len = metadata.large.len as usize;

                self.buffers[metadata.large.buffer_idx as usize]
                    .buffer
                    .as_slice_mut()
                    .get_mut(offset..(offset + len))
            }
        }
    }
}

#[derive(Debug)]
struct RowHeapBuffer<B: BufferManager> {
    /// Buffer containing all blob data.
    buffer: TypedRawBuffer<u8, B>,
    /// How much of the buffer is filled.
    filled: usize,
}

impl<B> RowHeapBuffer<B>
where
    B: BufferManager,
{
    fn with_capacity(manager: &B, capacity: usize) -> Result<Self> {
        let buffer = TypedRawBuffer::try_with_capacity(manager, capacity)?;
        Ok(RowHeapBuffer { buffer, filled: 0 })
    }

    const fn remaining_capacity(&self) -> usize {
        self.buffer.capacity() - self.filled
    }

    /// Try to reserve `additional` bytes for the buffer.
    fn reserve(&mut self, additional: usize) -> Result<()> {
        let mut new_size = self.buffer.capacity() * 2;
        if new_size == 0 {
            new_size = 16;
        }

        loop {
            if new_size + (self.buffer.capacity() - self.filled) > additional {
                // This is enough to fix `len` bytes.
                break;
            }
            // Otherwise try doubling.
            new_size *= 2;
        }

        self.buffer.reserve(new_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;

    #[test]
    fn push_many_small() {
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 4).unwrap();

        let m1 = heap.push_bytes(&[1, 2, 3]).unwrap();
        let m2 = heap.push_bytes(&[4, 5]).unwrap();
        let m3 = heap.push_bytes(&[6, 7, 8, 9]).unwrap();

        assert!(m1.is_small());
        assert!(m2.is_small());
        assert!(m3.is_small());

        assert_eq!(&[1, 2, 3], heap.get(&m1).unwrap());
        assert_eq!(&[4, 5], heap.get(&m2).unwrap());
        assert_eq!(&[6, 7, 8, 9], heap.get(&m3).unwrap());
    }

    #[test]
    fn push_many_large() {
        let mut heap = RowHeap::with_capacity(&NopBufferManager, 4).unwrap();

        let m1 = heap.push_bytes(&vec![1; 24]).unwrap();
        let m2 = heap.push_bytes(&vec![2; 32]).unwrap();
        let m3 = heap.push_bytes(&vec![3; 16]).unwrap();

        assert!(m1.is_large());
        assert!(m2.is_large());
        assert!(m3.is_large());

        assert_eq!(&vec![1; 24], heap.get(&m1).unwrap());
        assert_eq!(&vec![2; 32], heap.get(&m2).unwrap());
        assert_eq!(&vec![3; 16], heap.get(&m3).unwrap());
    }
}
