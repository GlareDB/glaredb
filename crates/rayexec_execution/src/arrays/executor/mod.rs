pub mod aggregate;
pub mod scalar;

use super::buffer::addressable::MutableAddressableStorage;
use super::validity::Validity;

/// Helper for assigning a value to a location in a buffer.
#[derive(Debug)]
pub struct OutputBuffer<'a, M>
where
    M: MutableAddressableStorage,
{
    idx: usize,
    buffer: &'a mut M,
    validity: &'a mut Validity,
}

impl<'a, M> OutputBuffer<'a, M>
where
    M: MutableAddressableStorage,
{
    pub(crate) fn new(idx: usize, buffer: &'a mut M, validity: &'a mut Validity) -> Self {
        debug_assert_eq!(buffer.len(), validity.len());
        OutputBuffer {
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
