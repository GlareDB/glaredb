use bytes::Bytes;

use super::AddressableStorage;

/// Back storage for shared byte buffers.
///
/// This mostly exists to allow us to use the byte blobs produced from parquet
/// directly with needing to copy it.
///
/// It's unknown if this will continue to exist long term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedHeapStorage {
    pub(crate) blobs: Vec<Bytes>,
}

impl SharedHeapStorage {
    pub fn with_capacity(cap: usize) -> Self {
        SharedHeapStorage {
            blobs: Vec::with_capacity(cap),
        }
    }

    pub fn get(&self, idx: usize) -> Option<&Bytes> {
        self.blobs.get(idx)
    }

    pub fn push(&mut self, blob: impl Into<Bytes>) {
        self.blobs.push(blob.into())
    }

    pub fn len(&self) -> usize {
        self.blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_size_bytes(&self) -> usize {
        self.blobs.iter().map(|b| b.len()).sum()
    }

    pub fn iter(&self) -> SharedHeapIter {
        SharedHeapIter {
            inner: self.blobs.iter(),
        }
    }

    pub fn as_shared_heap_storage_slice(&self) -> SharedHeapStorageSlice {
        SharedHeapStorageSlice { blobs: &self.blobs }
    }
}

impl From<Vec<Bytes>> for SharedHeapStorage {
    fn from(value: Vec<Bytes>) -> Self {
        SharedHeapStorage { blobs: value }
    }
}

#[derive(Debug)]
pub struct SharedHeapIter<'a> {
    inner: std::slice::Iter<'a, Bytes>,
}

impl<'a> Iterator for SharedHeapIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|b| b.as_ref())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a> ExactSizeIterator for SharedHeapIter<'a> {}

#[derive(Debug)]
pub struct SharedHeapStorageSlice<'a> {
    blobs: &'a [Bytes],
}

impl<'a> AddressableStorage for SharedHeapStorageSlice<'a> {
    type T = &'a [u8];

    fn len(&self) -> usize {
        self.blobs.len()
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        self.blobs.get(idx).map(|b| b.as_ref())
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        self.blobs.get_unchecked(idx).as_ref()
    }
}
