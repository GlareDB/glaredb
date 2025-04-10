use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalI64,
};
use glaredb_core::util::marker::PhantomCovariant;

use super::ValueReader;
use crate::column::read_buffer::ReadBuffer;

pub type PlainInt32ValueReader = PlainPrimitiveValueReader<PhysicalI32>;
pub type PlainInt64ValueReader = PlainPrimitiveValueReader<PhysicalI64>;

/// Value reader that can read primitive values and place them into the output
/// array with no conversion.
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
