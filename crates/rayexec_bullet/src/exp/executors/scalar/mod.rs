pub mod binary;
pub mod unary;

use crate::exp::buffer::addressable::MutableAddressableStorage;
use crate::exp::buffer::ArrayBuffer;

#[derive(Debug)]
pub struct OutputBuffer<'a, M>
where
    M: MutableAddressableStorage,
{
    idx: usize,
    buffer: &'a mut M,
}

impl<'a, M> OutputBuffer<'a, M>
where
    M: MutableAddressableStorage,
{
    pub fn put(self, val: &M::T) {
        self.buffer.put(self.idx, val)
    }
}
