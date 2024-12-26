pub mod aggregate;
pub mod scalar;

use super::buffer::addressable::MutableAddressableStorage;
use super::buffer::ArrayBuffer;
use super::validity::Validity;

/// Wrapper around an array buffer and validity buffer that will be used to
/// construct a full array.
#[derive(Debug)]
pub struct OutBuffer<'a> {
    pub buffer: &'a mut ArrayBuffer,
    pub validity: &'a mut Validity,
}

/// Helper for assigning a value to a location in a buffer.
#[derive(Debug)]
pub struct PutBuffer<'a, M>
where
    M: MutableAddressableStorage,
{
    idx: usize,
    buffer: &'a mut M,
    validity: &'a mut Validity,
}

impl<'a, M> PutBuffer<'a, M>
where
    M: MutableAddressableStorage,
{
    pub(crate) fn new(idx: usize, buffer: &'a mut M, validity: &'a mut Validity) -> Self {
        debug_assert_eq!(buffer.len(), validity.len());
        PutBuffer {
            idx,
            buffer,
            validity,
        }
    }

    pub fn put(self, val: &M::T) {
        self.buffer.put(self.idx, val)
    }

    pub fn put_null(self) {
        self.validity.set_invalid(self.idx)
    }
}
