use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
};
use glaredb_core::util::marker::PhantomCovariant;
use num::cast::AsPrimitive;

use super::ValueReader;
use crate::column::read_buffer::ReadCursor;

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
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut S::AddressableMut<'_>,
    ) {
        let v = unsafe { data.read_next_unchecked::<S::StorageType>() };
        out.put(out_idx, &v);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor) {
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<S::StorageType>()) };
    }
}

// INT32 => LogicalType::Int8
pub type CastingInt32ToInt8Reader = CastingValueReader<i32, PhysicalI8>;
// INT32 => LogicalType::Int16
pub type CastingInt32ToInt16Reader = CastingValueReader<i32, PhysicalI16>;
// INT32 => LogicalType::UInt8
pub type CastingInt32ToUInt8Reader = CastingValueReader<i32, PhysicalU8>;
// INT32 => LogicalType::UInt16
pub type CastingInt32ToUInt16Reader = CastingValueReader<i32, PhysicalU16>;
// INT32 => LogicalType::UInt32
pub type CastingInt32ToUInt32Reader = CastingValueReader<i32, PhysicalU32>;

// INT64 => LogicalType::UInt64
pub type CastingInt64ToUInt64Reader = CastingValueReader<i64, PhysicalU64>;

/// Value reader for reading primitive values, then casting them to the proper
/// storage type.
///
/// This is used when the parquet logical type indicates a type that's
/// physically different than the actual parquet storage type. E.g. INT32 =>
/// LogicalType::Int8.
///
/// Casting must be lossless.
#[derive(Debug, Clone, Copy, Default)]
pub struct CastingValueReader<T, S: MutableScalarStorage>
where
    T: AsPrimitive<S::StorageType>,
    S::StorageType: Copy,
{
    _t: PhantomCovariant<T>,
    _s: PhantomCovariant<S>,
}

impl<T, S> ValueReader for CastingValueReader<T, S>
where
    T: AsPrimitive<S::StorageType> + Default + Debug,
    S: MutableScalarStorage,
    S::StorageType: Copy,
{
    type Storage = S;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    ) {
        let v = unsafe { data.read_next_unchecked::<T>() };
        let v: S::StorageType = v.as_();
        out.put(out_idx, &v);
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor) {
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<T>()) };
    }
}
