use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBool,
};

use super::ValueReader;
use crate::column::read_buffer::ReadCursor;

/// Bit-packed bool reader (LSB).
#[derive(Debug, Default)]
pub struct BoolValueReader {
    /// Bit position within the byte.
    pos: usize,
}

impl ValueReader for BoolValueReader {
    type Storage = PhysicalBool;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    ) {
        let v = unsafe { data.peek_next_unchecked::<u8>() };
        let b = ((v >> self.pos) & 1) != 0;
        self.pos += 1;
        if self.pos == 8 {
            unsafe { data.skip_bytes_unchecked(1) };
            self.pos = 0;
        }

        out.put(out_idx, &b);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor) {
        self.pos += 1;
        if self.pos == 8 {
            unsafe { data.skip_bytes_unchecked(1) };
            self.pos = 0;
        }
    }
}
