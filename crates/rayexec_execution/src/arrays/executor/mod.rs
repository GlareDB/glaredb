pub mod aggregate;
pub mod scalar;

use rayexec_error::Result;

use super::array::array_buffer::ArrayBuffer;
use super::array::buffer_manager::{BufferManager, NopBufferManager};
use super::array::physical_type::AddressableMut;
use super::array::validity::Validity;
use super::array::Array;

/// Wrapper around an array buffer and validity buffer that will be used to
/// construct a full array.
#[derive(Debug)]
pub struct OutBuffer<'a, B: BufferManager = NopBufferManager> {
    pub buffer: &'a mut ArrayBuffer<B>,
    pub validity: &'a mut Validity,
}

impl<'a> OutBuffer<'a> {
    pub fn from_array(array: &'a mut Array) -> Result<Self> {
        Ok(OutBuffer {
            buffer: array.data.try_as_mut()?,
            validity: &mut array.validity,
        })
    }
}

/// Helper for assigning a value to a location in a buffer.
#[derive(Debug)]
pub struct PutBuffer<'a, M>
where
    M: AddressableMut,
{
    pub(crate) idx: usize,
    pub(crate) buffer: &'a mut M,
    pub(crate) validity: &'a mut Validity,
}

impl<'a, M> PutBuffer<'a, M>
where
    M: AddressableMut,
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
