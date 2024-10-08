use rayexec_error::Result;
use std::fmt::Debug;

use super::{AddressableStorage, PrimitiveStorage};

pub trait OffsetIndex: Debug + Clone + Copy + PartialEq + Eq {
    const ZERO: Self;

    fn get(start: Self, end: Self, slice: &[u8]) -> Option<&[u8]>;

    /// Unsafely slice a byte slice.
    ///
    /// # Safety
    ///
    /// UB if attempting to get a slice outside of `slice`.
    unsafe fn get_unchecked(start: Self, end: Self, slice: &[u8]) -> &[u8];

    fn from_usize(v: usize) -> Self;
}

impl OffsetIndex for i32 {
    const ZERO: Self = 0;

    fn get(start: Self, end: Self, slice: &[u8]) -> Option<&[u8]> {
        slice.get((start as usize)..(end as usize))
    }

    unsafe fn get_unchecked(start: Self, end: Self, slice: &[u8]) -> &[u8] {
        slice.get_unchecked((start as usize)..(end as usize))
    }

    fn from_usize(v: usize) -> Self {
        v as i32
    }
}

impl OffsetIndex for i64 {
    const ZERO: Self = 0;

    fn get(start: Self, end: Self, slice: &[u8]) -> Option<&[u8]> {
        slice.get((start as usize)..(end as usize))
    }

    unsafe fn get_unchecked(start: Self, end: Self, slice: &[u8]) -> &[u8] {
        slice.get_unchecked((start as usize)..(end as usize))
    }

    fn from_usize(v: usize) -> Self {
        v as i64
    }
}

/// Backing storage for multiple variable length values stored in a contiguous
/// vector.
///
/// This should be the backing storage for binary and (most) string data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContiguousVarlenStorage<O> {
    /// Offsets into the data buffer. The first value should be 0.
    pub(crate) offsets: PrimitiveStorage<O>,
    /// The data buffers being indexed into.
    pub(crate) data: PrimitiveStorage<u8>,
}

impl<O: OffsetIndex> ContiguousVarlenStorage<O> {
    pub fn with_offsets_and_data_capacity(offsets_cap: usize, data_cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(offsets_cap + 1);
        offsets.push(O::ZERO);
        let data: Vec<u8> = Vec::with_capacity(data_cap);

        ContiguousVarlenStorage {
            offsets: offsets.into(),
            data: data.into(),
        }
    }

    pub fn try_push(&mut self, value: &[u8]) -> Result<()> {
        let data = self.data.try_as_vec_mut()?;
        data.extend_from_slice(value);
        let offset = data.len();
        self.offsets.try_as_vec_mut()?.push(O::from_usize(offset));

        Ok(())
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        let start = self.offsets.as_ref().get(idx)?;
        let end = self.offsets.as_ref().get(idx + 1)?;

        O::get(*start, *end, self.data.as_ref())
    }

    pub fn len(&self) -> usize {
        self.offsets.as_ref().len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> ContiguousVarlenIter<'_, O> {
        ContiguousVarlenIter {
            storage: self,
            idx: 0,
        }
    }

    pub fn data_size_bytes(&self) -> usize {
        self.data.data_size_bytes()
    }

    pub fn as_contiguous_storage_slice(&self) -> ContiguousVarlenStorageSlice<O> {
        ContiguousVarlenStorageSlice {
            offsets: self.offsets.as_ref(),
            data: self.data.as_ref(),
        }
    }
}

impl<'a> FromIterator<&'a str> for ContiguousVarlenStorage<i32> {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let mut storage = ContiguousVarlenStorage::with_offsets_and_data_capacity(0, 0);
        for s in iter {
            storage
                .try_push(s.as_bytes())
                .expect("storage to be a vec (and pushable)");
        }

        storage
    }
}

#[derive(Debug)]
pub struct ContiguousVarlenIter<'a, O> {
    storage: &'a ContiguousVarlenStorage<O>,
    idx: usize,
}

impl<'a, O: OffsetIndex> Iterator for ContiguousVarlenIter<'a, O> {
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

impl<'a, O: OffsetIndex> ExactSizeIterator for ContiguousVarlenIter<'a, O> {}

#[derive(Debug)]
pub struct ContiguousVarlenStorageSlice<'a, O> {
    offsets: &'a [O],
    data: &'a [u8],
}

impl<'a, O: OffsetIndex> AddressableStorage for ContiguousVarlenStorageSlice<'a, O> {
    type T = &'a [u8];

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        let start = self.offsets.get(idx)?;
        let end = self.offsets.get(idx + 1)?;

        O::get(*start, *end, self.data)
    }

    #[inline]
    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        let start = self.offsets.get_unchecked(idx);
        let end = self.offsets.get_unchecked(idx + 1);

        O::get_unchecked(*start, *end, self.data)
    }
}
