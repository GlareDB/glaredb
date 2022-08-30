use bytes::{Buf, BufMut};

/// Encode self to some byte buffer.
pub trait Encodeable {
    /// Get the exact number of bytes that this instance will encode to.
    fn encode_size(&self) -> usize;

    /// Encode self to `dst`. The len of `dst` is guaranteed to be exactly the
    /// size reported from `encode_size`.
    fn encode<B: BufMut>(&self, dst: &mut B);
}

/// Decode self from some byte buffer.
pub trait Decodeable {
    /// Decode an instance of self from the provided buffer.
    ///
    /// Panics if `src` is too small.
    fn decode<B: Buf>(src: &mut B) -> Self;
}

pub trait BufMutExt {
    fn put_len_prefixed_slice(&mut self, slice: &[u8]);
}

impl<B: BufMut> BufMutExt for B {
    fn put_len_prefixed_slice(&mut self, slice: &[u8]) {
        self.put_u64(slice.len() as u64);
        self.put(slice);
    }
}
