use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
};

use super::ValueReader;
use crate::column::read_buffer::ReadCursor;

/// Value reader for reading variable length strings and byte arrays.
#[derive(Debug, Clone, Copy, Default)]
pub struct VarlenByteValueReader;

impl ValueReader for VarlenByteValueReader {
    type Storage = PhysicalBinary;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    ) {
        let len = unsafe { data.read_next_unchecked::<u32>() } as usize;
        let bs = unsafe { data.read_bytes_unchecked(len) };
        out.put(out_idx, bs);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor) {
        let len = unsafe { data.read_next_unchecked::<u32>() } as usize;
        unsafe { data.skip_bytes_unchecked(len) };
    }
}
