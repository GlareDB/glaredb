use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalI32,
    PhysicalI64,
};
use glaredb_core::util::marker::PhantomCovariant;

use crate::column::read_buffer::ReadBuffer;

/// Reads values from the read buffer, and writes them to addressable storage.
///
/// The buffers passed to this read are exact sized, and the index to write
/// values to should always be in bounds relative to the provided storage. This
/// should never error.
///
/// A new value reader is created every time a new page is loaded (via
/// `Default`).
///
/// # Safety
///
/// The caller must guarantee that we have sufficient data in the buffer to read
/// a complete value.
pub trait ValueReader: Default + Debug + Sync + Send {
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

pub type PlainInt32ValueReader = PlainPrimitiveValueReader<PhysicalI32>;
pub type PlainInt64ValueReader = PlainPrimitiveValueReader<PhysicalI64>;

#[derive(Debug, Clone, Copy, Default)]
pub struct PlainPrimitiveValueReader<S: MutableScalarStorage> {
    _s: PhantomCovariant<S>,
}

impl<S> ValueReader for PlainPrimitiveValueReader<S>
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
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<S::StorageType>()) };
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PlainByteArrayValueReader;

impl ValueReader for PlainByteArrayValueReader {
    type Storage = PhysicalBinary;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadBuffer,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    ) {
        let len = unsafe { data.read_next_unchecked::<u32>() } as usize;
        let bs = unsafe { data.read_bytes_unchecked(len) };
        out.put(out_idx, bs);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadBuffer) {
        let len = unsafe { data.read_next_unchecked::<u32>() } as usize;
        unsafe { data.skip_bytes_unchecked(len) };
    }
}
