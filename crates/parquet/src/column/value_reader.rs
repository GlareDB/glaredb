#![allow(unused)]

use std::fmt::Debug;

use stdutil::marker::PhantomCovariant;

use crate::column::read_buffer::ReadBuffer;

/// Describes reading a value from a read buffer.
///
/// The buffers passed to this read are exact sized. This should never error.
pub trait ValueReader: Debug + Sync + Send {
    /// Type of the value we read from the buffer.
    type T: Copy;

    /// Read the next value in the buffer.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that we have sufficient data in the buffer to
    /// read a complete value.
    unsafe fn read_unchecked(&mut self, data: &mut ReadBuffer) -> Self::T;

    /// Skip the next value in the buffer.
    ///
    /// # Safety
    ///
    /// See `read_unchecked`.
    unsafe fn skip_unchecked(&mut self, data: &mut ReadBuffer);
}

#[derive(Debug, Clone, Copy)]
pub struct PrimitiveValueReader<T> {
    _t: PhantomCovariant<T>,
}

impl<T> PrimitiveValueReader<T> {
    pub const fn new() -> Self {
        PrimitiveValueReader {
            _t: PhantomCovariant::new(),
        }
    }
}

impl<T> ValueReader for PrimitiveValueReader<T>
where
    T: Copy + Debug,
{
    type T = T;

    unsafe fn read_unchecked(&mut self, data: &mut ReadBuffer) -> Self::T {
        data.read_next_unchecked::<Self::T>()
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadBuffer) {
        data.skip_bytes_unchecked(std::mem::size_of::<Self::T>());
    }
}
