use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut, MutableScalarStorage, PhysicalF16, PhysicalF32, PhysicalF64, PhysicalI8,
    PhysicalI16, PhysicalI32, PhysicalI64, PhysicalU8, PhysicalU16, PhysicalU32, PhysicalU64,
};
use glaredb_core::util::marker::PhantomCovariant;
use num::cast::AsPrimitive;

use super::{ReaderErrorState, ValueReader};
use crate::column::read_buffer::ReadCursor;
use crate::column::row_group_pruner::{
    PlainType, PlainTypeF32, PlainTypeF64, PlainTypeFixedLenByteArray, PlainTypeI32, PlainTypeI64,
};

pub type PlainInt32ValueReader = PrimitiveValueReader<PhysicalI32, PlainTypeI32>;
pub type PlainInt64ValueReader = PrimitiveValueReader<PhysicalI64, PlainTypeI64>;
pub type PlainFloat32ValueReader = PrimitiveValueReader<PhysicalF32, PlainTypeF32>;
pub type PlainFloat64ValueReader = PrimitiveValueReader<PhysicalF64, PlainTypeF64>;

// FIXED_SIZE_BYTE_ARRAY(2) => Float16
// Just read directly from the byte stream (little endian)
pub type PlainFloat16ValueReader = PrimitiveValueReader<PhysicalF16, PlainTypeFixedLenByteArray>;

// No conversion needed to read INT64 as ns.
pub type PlainTsNsValueReader = PrimitiveValueReader<PhysicalI64, PlainTypeI64>;
// No conversion needed to read INT64 as micros.
pub type PlainTsMicrosValueReader = PrimitiveValueReader<PhysicalI64, PlainTypeI64>;

/// Value reader that can read primitive values and place them into the output
/// array with no conversion.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrimitiveValueReader<S: MutableScalarStorage, T: PlainType> {
    _s: PhantomCovariant<S>,
    _t: PhantomCovariant<T>,
}

impl<S, T> ValueReader for PrimitiveValueReader<S, T>
where
    S: MutableScalarStorage,
    S::StorageType: Copy + Sized,
    T: PlainType,
{
    type Storage = S;
    type PlainType = T;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut S::AddressableMut<'_>,
        _error_state: &mut ReaderErrorState,
    ) {
        let v = unsafe { data.read_next_unchecked::<S::StorageType>() };
        out.put(out_idx, &v);
    }

    unsafe fn skip_unchecked(
        &mut self,
        data: &mut ReadCursor,
        _error_state: &mut ReaderErrorState,
    ) {
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<S::StorageType>()) };
    }
}

// INT32 => LogicalType::Int8
pub type CastingInt32ToInt8Reader = CastingValueReader<PlainTypeI32, PhysicalI8>;
// INT32 => LogicalType::Int16
pub type CastingInt32ToInt16Reader = CastingValueReader<PlainTypeI32, PhysicalI16>;
// INT32 => LogicalType::UInt8
pub type CastingInt32ToUInt8Reader = CastingValueReader<PlainTypeI32, PhysicalU8>;
// INT32 => LogicalType::UInt16
pub type CastingInt32ToUInt16Reader = CastingValueReader<PlainTypeI32, PhysicalU16>;
// INT32 => LogicalType::UInt32
pub type CastingInt32ToUInt32Reader = CastingValueReader<PlainTypeI32, PhysicalU32>;

// INT32 => LogicalType::Int64 (for Decimal64)
pub type CastingInt32ToInt64Reader = CastingValueReader<PlainTypeI32, PhysicalI64>;

// INT64 => LogicalType::UInt64
pub type CastingInt64ToUInt64Reader = CastingValueReader<PlainTypeI64, PhysicalU64>;

/// Value reader for reading primitive values, then casting them to the proper
/// storage type.
///
/// This is used when the parquet logical type indicates a type that's
/// physically different than the actual parquet storage type. E.g. INT32 =>
/// LogicalType::Int8.
///
/// Casting must be lossless.
#[derive(Debug, Clone, Copy, Default)]
pub struct CastingValueReader<T: PlainType, S: MutableScalarStorage>
where
    T::Native: AsPrimitive<S::StorageType>,
    S::StorageType: Copy,
{
    _t: PhantomCovariant<T>,
    _s: PhantomCovariant<S>,
}

impl<T, S> ValueReader for CastingValueReader<T, S>
where
    T: PlainType,
    T::Native: AsPrimitive<S::StorageType> + Default + Debug,
    S: MutableScalarStorage,
    S::StorageType: Copy,
{
    type Storage = S;
    type PlainType = T;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
        _error_state: &mut ReaderErrorState,
    ) {
        let v = unsafe { data.read_next_unchecked::<T::Native>() };
        let v: S::StorageType = v.as_();
        out.put(out_idx, &v);
    }

    unsafe fn skip_unchecked(
        &mut self,
        data: &mut ReadCursor,
        _error_state: &mut ReaderErrorState,
    ) {
        unsafe { data.skip_bytes_unchecked(std::mem::size_of::<T>()) };
    }
}
