use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::{AddressableMut, MutableScalarStorage};
use glaredb_core::util::marker::PhantomCovariant;

use crate::column::read_buffer::ReadBuffer;

/// Reads values from the read buffer, and writes them to addressable storage.
///
/// The buffers passed to this read are exact sized, and the index to write
/// values to should always be in bounds relative to the provided storage. This
/// should never error.
///
/// # Safety
///
/// The caller must guarantee that we have sufficient data in the buffer to read
/// a complete value.
pub trait ValueReader: Debug + Sync + Send {
    type Storage: MutableScalarStorage;

    /// Read the next value in the buffer, writing it the mutable storage at the
    /// given index.
    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadBuffer,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    );

    /// Skip the next value in the buffer.
    unsafe fn skip_unchecked(&mut self, data: &mut ReadBuffer);
}

#[derive(Debug, Clone, Copy)]
pub struct PrimitiveValueReader<S: MutableScalarStorage> {
    _s: PhantomCovariant<S>,
}

impl<S> PrimitiveValueReader<S>
where
    S: MutableScalarStorage,
{
    pub const fn new() -> Self {
        PrimitiveValueReader {
            _s: PhantomCovariant::new(),
        }
    }
}

impl<S> ValueReader for PrimitiveValueReader<S>
where
    S: MutableScalarStorage,
    S::StorageType: Copy + Sized,
{
    type Storage = S;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadBuffer,
        out_idx: usize,
        out: &mut S::AddressableMut<'_>,
    ) {
        let v = unsafe { data.read_next_unchecked::<S::StorageType>() };
        out.put(out_idx, &v);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadBuffer) {
        unsafe {
            data.skip_bytes_unchecked(std::mem::size_of::<S::StorageType>());
        }
    }
}
