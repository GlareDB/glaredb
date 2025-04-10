use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalF32,
    PhysicalF64,
    PhysicalI32,
    PhysicalI64,
};
use glaredb_core::util::marker::PhantomCovariant;

use super::ValueReader;
use crate::column::read_buffer::ReadBuffer;

pub type PlainInt32ValueReader = PrimitiveValueReader<PhysicalI32>;
pub type PlainInt64ValueReader = PrimitiveValueReader<PhysicalI64>;
pub type PlainFloat32ValueReader = PrimitiveValueReader<PhysicalF32>;
pub type PlainFloat64ValueReader = PrimitiveValueReader<PhysicalF64>;

// No conversion needed to read INT64 as ns.
pub type PlainTsNsValueReader = PrimitiveValueReader<PhysicalI64>;

/// Value reader that can read primitive values and place them into the output
/// array with no conversion.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrimitiveValueReader<S: MutableScalarStorage> {
    _s: PhantomCovariant<S>,
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
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<S::StorageType>()) };
    }
}
