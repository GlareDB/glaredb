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
    /// Buffer containing all blob data.
    buffer: TypedRawBuffer<u8, B>,
    /// How much of the buffer is filled.
    filled: usize,
}

impl<B> RowHeap<B>
where
    B: BufferManager,
{
    /// Create a new heap with an initial capacity.
    pub fn with_capacity(manager: &B, capacity: usize) -> Result<Self> {
        let buffer = TypedRawBuffer::try_with_capacity(manager, capacity)?;
        Ok(RowHeap { buffer, filled: 0 })
    }

    pub fn clear(&mut self) {
        self.filled = 0;
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

            if self.buffer.capacity() - self.filled < value.len() {
                self.resize_buffer(value.len())?;
            }

            let offset = self.filled;

            // Copy prefix to metadata.
            let mut prefix = [0; 4];
            let prefix_len = std::cmp::min(value.len(), 4);
            prefix[0..prefix_len].copy_from_slice(&value[0..prefix_len]);

            // Copy entire value to buffer.
            let buf = &mut self.buffer.as_mut()[self.filled..(self.filled + value.len())];
            buf.copy_from_slice(value);

            self.filled += value.len();

            Ok(RowHeapLargeMetadata {
                len: value.len() as i32,
                prefix,
                buffer_idx: 0,
                offset: offset as i32,
            }
            .into())
        }
    }

    pub fn get<'a, 'b: 'a>(&'b self, metadata: &'a RowHeapMetadataUnion) -> Option<&'a [u8]> {
        if metadata.is_small() {
            unsafe { Some(&metadata.small.inline[..(metadata.small.len as usize)]) }
        } else {
            unsafe {
                let offset = metadata.large.offset as usize;
                let len = metadata.large.len as usize;

                self.buffer.as_slice().get(offset..(offset + len))
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

                self.buffer.as_slice_mut().get_mut(offset..(offset + len))
            }
        }
    }

    /// Try to resize the buffer to fix `len` additional bytes.
    fn resize_buffer(&mut self, len: usize) -> Result<()> {
        let mut additional = self.buffer.capacity() * 2;
        if additional == 0 {
            additional = 16;
        }

        loop {
            if additional + (self.buffer.capacity() - self.filled) > len {
                // This is enough to fix `len` bytes.
                break;
            }
            // Otherwise try doubling.
            additional *= 2;
        }

        self.buffer.reserve(additional)
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
