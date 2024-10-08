use std::fmt;

use rayexec_error::Result;

use super::{AddressableStorage, PrimitiveStorage};
use crate::executor::physical_type::VarlenType;

/// Byte length threshold for inlining varlen data in the array's metadata.
pub(crate) const INLINE_THRESHOLD: i32 = 12;

/// Metadata for small (<= 12 bytes) varlen data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct GermanSmallMetadata {
    pub len: i32,
    pub inline: [u8; 12],
}

/// Metadata for large (> 12 bytes) varlen data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct GermanLargeMetadata {
    pub len: i32,
    pub prefix: [u8; 4],
    pub buffer_idx: i32,
    pub offset: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GermanMetadata<'a> {
    Small(&'a GermanSmallMetadata),
    Large(&'a GermanLargeMetadata),
}

#[derive(Clone, Copy)]
#[repr(C)]
pub union UnionedGermanMetadata {
    small: GermanSmallMetadata,
    large: GermanLargeMetadata,
}

impl Default for UnionedGermanMetadata {
    fn default() -> Self {
        Self::zero()
    }
}

impl UnionedGermanMetadata {
    pub fn as_metadata(&self) -> GermanMetadata {
        unsafe {
            // i32 len is first field in both, safe to access from either
            // variant.
            if self.small.len <= INLINE_THRESHOLD {
                GermanMetadata::Small(&self.small)
            } else {
                GermanMetadata::Large(&self.large)
            }
        }
    }

    pub fn data_len(&self) -> i32 {
        // SAFETY: `len` field is in the same place in both variants.
        unsafe { self.small.len }
    }

    pub(crate) fn as_small_mut(&mut self) -> &mut GermanSmallMetadata {
        unsafe { &mut self.small }
    }

    pub(crate) fn as_large_mut(&mut self) -> &mut GermanLargeMetadata {
        unsafe { &mut self.large }
    }

    pub(crate) const fn zero() -> Self {
        Self {
            small: GermanSmallMetadata {
                len: 0,
                inline: [0; 12],
            },
        }
    }
}

impl fmt::Debug for UnionedGermanMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_metadata().fmt(f)
    }
}

impl PartialEq for UnionedGermanMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.as_metadata().eq(&other.as_metadata())
    }
}

impl Eq for UnionedGermanMetadata {}

impl From<GermanSmallMetadata> for UnionedGermanMetadata {
    fn from(value: GermanSmallMetadata) -> Self {
        UnionedGermanMetadata { small: value }
    }
}

impl From<GermanLargeMetadata> for UnionedGermanMetadata {
    fn from(value: GermanLargeMetadata) -> Self {
        UnionedGermanMetadata { large: value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GermanVarlenStorage {
    pub(crate) metadata: PrimitiveStorage<UnionedGermanMetadata>,
    pub(crate) data: PrimitiveStorage<u8>,
}

impl GermanVarlenStorage {
    pub fn with_value<V>(val: &V) -> Self
    where
        V: VarlenType + ?Sized,
    {
        let bs = val.as_bytes();
        let mut s = Self::with_metadata_capacity(1);
        s.try_push(bs).unwrap();

        s
    }

    pub fn with_metadata_capacity(meta_cap: usize) -> Self {
        GermanVarlenStorage {
            metadata: Vec::with_capacity(meta_cap).into(),
            data: Vec::new().into(),
        }
    }

    pub fn len(&self) -> usize {
        self.metadata.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_push(&mut self, value: &[u8]) -> Result<()> {
        let metadata = self.metadata.try_as_vec_mut()?;
        let data = self.data.try_as_vec_mut()?;

        if value.len() as i32 <= INLINE_THRESHOLD {
            // Store completely inline.
            let mut inline = [0; 12];
            inline[0..value.len()].copy_from_slice(value);

            metadata.push(
                GermanSmallMetadata {
                    len: value.len() as i32,
                    inline,
                }
                .into(),
            );
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.

            let offset = data.len();
            let mut prefix = [0; 4];
            let prefix_len = std::cmp::min(value.len(), 4);
            prefix[0..prefix_len].copy_from_slice(&value[0..prefix_len]);

            data.extend_from_slice(value);

            metadata.push(
                GermanLargeMetadata {
                    len: value.len() as i32,
                    prefix,
                    buffer_idx: 0,
                    offset: offset as i32,
                }
                .into(),
            )
        }

        Ok(())
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        let metadata = self.metadata.as_ref().get(idx)?;

        match metadata.as_metadata() {
            GermanMetadata::Small(GermanSmallMetadata { len, inline }) => {
                Some(&inline[..(*len as usize)])
            }
            GermanMetadata::Large(GermanLargeMetadata { len, offset, .. }) => {
                Some(&self.data.as_ref()[(*offset as usize)..((offset + len) as usize)])
            }
        }
    }

    pub fn iter(&self) -> GermanVarlenIter {
        GermanVarlenIter {
            storage: self,
            idx: 0,
        }
    }

    pub fn data_size_bytes(&self) -> usize {
        self.metadata.iter().map(|m| m.data_len() as usize).sum()
    }

    pub fn as_german_storage_slice(&self) -> GermanVarlenStorageSlice {
        GermanVarlenStorageSlice {
            metadata: self.metadata.as_ref(),
            data: self.data.as_ref(),
        }
    }
}

#[derive(Debug)]
pub struct GermanVarlenIter<'a> {
    storage: &'a GermanVarlenStorage,
    idx: usize,
}

impl<'a> Iterator for GermanVarlenIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let v = self.storage.get(self.idx)?;
        self.idx += 1;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.storage.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for GermanVarlenIter<'a> {}

#[derive(Debug)]
pub struct GermanVarlenStorageSlice<'a> {
    metadata: &'a [UnionedGermanMetadata],
    data: &'a [u8],
}

impl<'a> AddressableStorage for GermanVarlenStorageSlice<'a> {
    type T = &'a [u8];

    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        let metadata = self.metadata.get(idx)?;

        match metadata.as_metadata() {
            GermanMetadata::Small(GermanSmallMetadata { len, inline }) => {
                Some(&inline[..(*len as usize)])
            }
            GermanMetadata::Large(GermanLargeMetadata { len, offset, .. }) => {
                Some(&self.data[(*offset as usize)..((offset + len) as usize)])
            }
        }
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        self.get(idx).unwrap()
    }
}
