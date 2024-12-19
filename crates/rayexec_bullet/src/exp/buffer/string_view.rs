use std::fmt;

use super::addressable::{AddressableStorage, MutableAddressableStorage};

#[derive(Debug)]
pub struct StringViewBuffer<'a> {
    pub(crate) metadata: &'a [StringViewMetadataUnion],
    pub(crate) heap: &'a StringViewHeap,
}

impl<'a> AddressableStorage for StringViewBuffer<'a> {
    type T = str;

    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn get(&self, idx: usize) -> Option<&Self::T> {
        let m = self.metadata.get(idx)?;
        let bs = self.heap.get(m)?;
        Some(unsafe { std::str::from_utf8_unchecked(bs) })
    }
}

#[derive(Debug)]
pub struct StringViewBufferMut<'a> {
    pub(crate) metadata: &'a mut [StringViewMetadataUnion],
    pub(crate) heap: &'a mut StringViewHeap,
}

impl<'a> AddressableStorage for StringViewBufferMut<'a> {
    type T = str;

    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn get(&self, idx: usize) -> Option<&Self::T> {
        let m = self.metadata.get(idx)?;
        let bs = self.heap.get(m)?;
        Some(unsafe { std::str::from_utf8_unchecked(bs) })
    }
}

impl<'a> MutableAddressableStorage for StringViewBufferMut<'a> {
    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        let m = self.metadata.get_mut(idx)?;
        let bs = self.heap.get_mut(m)?;
        Some(unsafe { std::str::from_utf8_unchecked_mut(bs) })
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        let bs = val.as_bytes();
        let new_m = self.heap.push_bytes(bs);
        self.metadata[idx] = new_m;
    }
}

/// Metadata for small (<= 12 bytes) varlen data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StringViewSmallMetadata {
    pub len: i32,
    pub inline: [u8; 12],
}

/// Metadata for large (> 12 bytes) varlen data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StringViewLargeMetadata {
    pub len: i32,
    pub prefix: [u8; 4],
    pub buffer_idx: i32,
    pub offset: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringViewMetadata<'a> {
    Small(&'a StringViewSmallMetadata),
    Large(&'a StringViewLargeMetadata),
}

#[derive(Clone, Copy)]
#[repr(C)]
pub union StringViewMetadataUnion {
    small: StringViewSmallMetadata,
    large: StringViewLargeMetadata,
}

impl Default for StringViewMetadataUnion {
    #[inline]
    fn default() -> Self {
        Self::zero()
    }
}

impl StringViewMetadataUnion {
    #[inline]
    pub fn as_metadata(&self) -> StringViewMetadata {
        unsafe {
            // i32 len is first field in both, safe to access from either
            // variant.
            if self.is_small() {
                StringViewMetadata::Small(&self.small)
            } else {
                StringViewMetadata::Large(&self.large)
            }
        }
    }

    pub const fn is_small(&self) -> bool {
        // i32 len is first field in both, safe to access from either
        // variant.
        unsafe { self.small.len <= 12 }
    }

    pub fn data_len(&self) -> i32 {
        // SAFETY: `len` field is in the same place in both variants.
        unsafe { self.small.len }
    }

    pub(crate) const fn zero() -> Self {
        Self {
            small: StringViewSmallMetadata {
                len: 0,
                inline: [0; 12],
            },
        }
    }

    fn as_small(&self) -> StringViewSmallMetadata {
        debug_assert!(self.is_small());
        unsafe { self.small }
    }

    fn as_large(&self) -> StringViewLargeMetadata {
        debug_assert!(!self.is_small());
        unsafe { self.large }
    }
}

impl From<StringViewSmallMetadata> for StringViewMetadataUnion {
    fn from(value: StringViewSmallMetadata) -> Self {
        StringViewMetadataUnion { small: value }
    }
}

impl From<StringViewLargeMetadata> for StringViewMetadataUnion {
    fn from(value: StringViewLargeMetadata) -> Self {
        StringViewMetadataUnion { large: value }
    }
}

impl fmt::Debug for StringViewMetadataUnion {
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
pub struct StringViewHeap {
    /// Buffer containing all blob data.
    buffer: Vec<u8>,
}

impl StringViewHeap {
    // TODO: Tracker
    pub const fn new() -> Self {
        StringViewHeap { buffer: Vec::new() }
    }

    pub fn push_bytes(&mut self, value: &[u8]) -> StringViewMetadataUnion {
        if value.len() as i32 <= 12 {
            // Store completely inline.
            let mut inline = [0; 12];
            inline[0..value.len()].copy_from_slice(value);

            StringViewSmallMetadata {
                len: value.len() as i32,
                inline,
            }
            .into()
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.

            let offset = self.buffer.len();
            let mut prefix = [0; 4];
            let prefix_len = std::cmp::min(value.len(), 4);
            prefix[0..prefix_len].copy_from_slice(&value[0..prefix_len]);

            self.buffer.extend_from_slice(value);

            StringViewLargeMetadata {
                len: value.len() as i32,
                prefix,
                buffer_idx: 0,
                offset: offset as i32,
            }
            .into()
        }
    }

    pub fn get<'a, 'b: 'a>(&'b self, metadata: &'a StringViewMetadataUnion) -> Option<&'a [u8]> {
        if metadata.is_small() {
            unsafe { Some(&metadata.small.inline[..(metadata.small.len as usize)]) }
        } else {
            unsafe {
                let offset = metadata.large.offset as usize;
                let len = metadata.large.len as usize;

                self.buffer.get(offset..(offset + len))
            }
        }
    }

    pub fn get_mut<'a, 'b: 'a>(
        &'b mut self,
        metadata: &'a mut StringViewMetadataUnion,
    ) -> Option<&'a mut [u8]> {
        if metadata.is_small() {
            unsafe { Some(&mut metadata.small.inline[..(metadata.small.len as usize)]) }
        } else {
            unsafe {
                let offset = metadata.large.offset as usize;
                let len = metadata.large.len as usize;

                self.buffer.get_mut(offset..(offset + len))
            }
        }
    }
}
