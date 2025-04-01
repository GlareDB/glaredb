use std::fmt::Display;
use std::marker::PhantomData;
use std::str::FromStr;

use glaredb_error::{DbError, Result};
use num_traits::{Float, NumCast, ToPrimitive};

use super::null::NullToAnything;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::{
    CastFunction,
    CastFunctionSet,
    CastRule,
    RawCastFunction,
    TO_F16_CAST_RULE,
    TO_F32_CAST_RULE,
    TO_F64_CAST_RULE,
    TO_INT8_CAST_RULE,
    TO_INT16_CAST_RULE,
    TO_INT32_CAST_RULE,
    TO_INT64_CAST_RULE,
    TO_UINT16_CAST_RULE,
    TO_UINT32_CAST_RULE,
    TO_UINT64_CAST_RULE,
};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_INT8: CastFunctionSet = CastFunctionSet {
    name: "to_int8",
    target: DataTypeId::Int8,
    #[rustfmt::skip]
    functions: &[
        // Null -> Int8
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_INT8_CAST_RULE),
        // Utf8 -> Int8
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalI8>::new(), TO_INT8_CAST_RULE),
        // Int_ -> Int8
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI8>::new(), TO_INT8_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI8>::new(), CastRule::Explicit),
        // UInt_ -> Int8
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI8>::new(), CastRule::Explicit),
        // Float_ -> Int8
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalI8>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_UINT8: CastFunctionSet = CastFunctionSet {
    name: "to_uint8",
    target: DataTypeId::UInt8,
    #[rustfmt::skip]
    functions: &[
        // Null -> UInt8
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_UINT16_CAST_RULE),
        // Utf8 -> UInt8
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalU8>::new(), CastRule::Explicit),
        // Int_ -> Uint8
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalU8>::new(), CastRule::Explicit),
        // UInt_ -> Uint8
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalU8>::new(), CastRule::Explicit),
        // Float_ -> Uint8
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalU8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalU8>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_INT16: CastFunctionSet = CastFunctionSet {
    name: "to_int16",
    target: DataTypeId::Int16,
    #[rustfmt::skip]
    functions: &[
        // Null -> Int16
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_INT16_CAST_RULE),
        // Utf8 -> Int16
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalI16>::new(), TO_INT16_CAST_RULE),
        // Int_ -> Int16
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI16>::new(), TO_INT16_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI16>::new(), TO_INT16_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI16>::new(), CastRule::Explicit),
        // UInt_ -> Int16
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI16>::new(), TO_INT16_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI16>::new(), CastRule::Explicit),
        // Float_ -> Int16
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalI16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalI16>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_UINT16: CastFunctionSet = CastFunctionSet {
    name: "to_uint16",
    target: DataTypeId::UInt16,
    #[rustfmt::skip]
    functions: &[
        // Null -> UInt16
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_UINT16_CAST_RULE),
        // Utf8 -> UInt16
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalU16>::new(), CastRule::Explicit),
        // Int_ -> Uint16
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalU16>::new(), CastRule::Explicit),
        // UInt_ -> Uint16
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalU16>::new(), TO_UINT16_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalU16>::new(), TO_UINT16_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalU16>::new(), CastRule::Explicit),
        // Float_ -> Uint16
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalU16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalU16>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_INT32: CastFunctionSet = CastFunctionSet {
    name: "to_int32",
    target: DataTypeId::Int32,
    #[rustfmt::skip]
    functions: &[
        // Null -> Int32
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_INT32_CAST_RULE),
        // Utf8 -> Int32
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalI32>::new(), TO_INT32_CAST_RULE),
        // Int_ -> Int32
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI32>::new(), TO_INT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI32>::new(), TO_INT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI32>::new(), TO_INT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI32>::new(), CastRule::Explicit),
        // UInt_ -> Int32
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI32>::new(), TO_INT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI32>::new(), TO_INT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI32>::new(), CastRule::Explicit),
        // Float_ -> Int32
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalI32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalI32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalI32>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_UINT32: CastFunctionSet = CastFunctionSet {
    name: "to_uint32",
    target: DataTypeId::UInt32,
    #[rustfmt::skip]
    functions: &[
        // Null -> UInt32
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_UINT32_CAST_RULE),
        // Utf8 -> UInt32
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalU32>::new(), CastRule::Explicit),
        // Int_ -> Uint32
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalU32>::new(), CastRule::Explicit),
        // UInt_ -> Uint32
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalU32>::new(), TO_UINT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalU32>::new(), TO_UINT32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalU32>::new(), CastRule::Explicit),
        // Float_ -> Uint32
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalU32>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalU32>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_INT64: CastFunctionSet = CastFunctionSet {
    name: "to_int64",
    target: DataTypeId::Int64,
    #[rustfmt::skip]
    functions: &[
        // Null -> Int64
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_INT64_CAST_RULE),
        // Utf8 -> Int64
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalI64>::new(), TO_INT64_CAST_RULE),
        // Int_ -> Int64
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI64>::new(), CastRule::Explicit),
        // UInt_ -> Int64
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI64>::new(), TO_INT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI64>::new(), CastRule::Explicit),
        // Float_ -> Int64
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalI64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalI64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalI64>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_UINT64: CastFunctionSet = CastFunctionSet {
    name: "to_uint64",
    target: DataTypeId::UInt64,
    #[rustfmt::skip]
    functions: &[
        // Null -> UInt64
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_UINT64_CAST_RULE),
        // Utf8 -> UInt64
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalU64>::new(), CastRule::Explicit),
        // Int_ -> Uint64
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalU64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalU64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalU64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalU64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalU64>::new(), CastRule::Explicit),
        // UInt_ -> Uint64
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalU64>::new(), TO_UINT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalU64>::new(), TO_UINT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalU64>::new(), TO_UINT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalU64>::new(), TO_UINT64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalU64>::new(), CastRule::Explicit),
        // Float_ -> Uint64
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalU64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalU64>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalU64>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_INT128: CastFunctionSet = CastFunctionSet {
    name: "to_int128",
    target: DataTypeId::Int128,
    #[rustfmt::skip]
    functions: &[
        // Null -> Int128
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, CastRule::Explicit),
        // Utf8 -> Int128
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalI128>::new(), CastRule::Explicit),
        // Int_ -> Int128
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI128>::new(), CastRule::Explicit),
        // UInt_ -> Int128
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI128>::new(), CastRule::Explicit),
        // Float_ -> Int128
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalI128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalI128>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_UINT128: CastFunctionSet = CastFunctionSet {
    name: "to_uint128",
    target: DataTypeId::UInt128,
    #[rustfmt::skip]
    functions: &[
        // Null -> UInt128
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, CastRule::Explicit),
        // Utf8 -> UInt128
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalU128>::new(), CastRule::Explicit),
        // Int_ -> Int128
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalU128>::new(), CastRule::Explicit),
        // UInt_ -> UInt128
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalU128>::new(), CastRule::Explicit),
        // Float_ -> UInt128
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalU128>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalU128>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_FLOAT16: CastFunctionSet = CastFunctionSet {
    name: "to_float16",
    target: DataTypeId::Float16,
    #[rustfmt::skip]
    functions: &[
        // Null -> Float16
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_F16_CAST_RULE),
        // Utf8 -> Float16
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalF16>::new(), CastRule::Explicit),
        // Int_ -> Float16
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalF16>::new(), TO_F16_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalF16>::new(), TO_F16_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalF16>::new(), TO_F16_CAST_RULE), // TODO: This might a bit sketch.
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalF16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalF16>::new(), CastRule::Explicit),
        // UInt_ -> Float16
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalF16>::new(), TO_F16_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalF16>::new(), TO_F16_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalF16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalF16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalF16>::new(), CastRule::Explicit),
        // Float_ -> Float16
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalF16>::new(), TO_F16_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalF16>::new(), TO_F16_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalF16>::new(), TO_F16_CAST_RULE),
        // Decimal_ -> Float16
        RawCastFunction::new(DataTypeId::Decimal64, &DecimalToFloat::<Decimal64Type, PhysicalF16>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Decimal128, &DecimalToFloat::<Decimal128Type, PhysicalF16>::new(), CastRule::Explicit),
    ],
};

pub const FUNCTION_SET_TO_FLOAT32: CastFunctionSet = CastFunctionSet {
    name: "to_float32",
    target: DataTypeId::Float32,
    #[rustfmt::skip]
    functions: &[
        // Null -> Float32
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_F32_CAST_RULE),
        // Utf8 -> Float32
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalF32>::new(), TO_F32_CAST_RULE),
        // Int_ -> Float32
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalF32>::new(), TO_F32_CAST_RULE),
        // UInt_ -> Float32
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalF32>::new(), TO_F32_CAST_RULE),
        // Float_ -> Float32
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalF32>::new(), TO_F32_CAST_RULE),
        // Decimal_ -> Float32
        RawCastFunction::new(DataTypeId::Decimal64, &DecimalToFloat::<Decimal64Type, PhysicalF32>::new(), TO_F32_CAST_RULE),
        RawCastFunction::new(DataTypeId::Decimal128, &DecimalToFloat::<Decimal128Type, PhysicalF32>::new(), TO_F32_CAST_RULE),
    ],
};

pub const FUNCTION_SET_TO_FLOAT64: CastFunctionSet = CastFunctionSet {
    name: "to_float64",
    target: DataTypeId::Float64,
    #[rustfmt::skip]
    functions: &[
        // Null -> Float64
        RawCastFunction::new(DataTypeId::Null, &NullToAnything, TO_F64_CAST_RULE),
        // Utf8 -> Float64
        RawCastFunction::new(DataTypeId::Utf8, &Utf8ToPrim::<PhysicalF64>::new(), TO_F64_CAST_RULE),
        // Int_ -> Float64
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalF64>::new(), TO_F64_CAST_RULE),
        // UInt_ -> Float64
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalF64>::new(), TO_F64_CAST_RULE),
        // Float_ -> Float64
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalF64>::new(), TO_F64_CAST_RULE),
        // Decimal_ -> Float64
        RawCastFunction::new(DataTypeId::Decimal64, &DecimalToFloat::<Decimal64Type, PhysicalF64>::new(), TO_F64_CAST_RULE),
        RawCastFunction::new(DataTypeId::Decimal128, &DecimalToFloat::<Decimal128Type, PhysicalF64>::new(), TO_F64_CAST_RULE),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct PrimToPrim<S1, S2> {
    _s1: PhantomData<S1>,
    _s2: PhantomData<S2>,
}

impl<S1, S2> PrimToPrim<S1, S2> {
    pub const fn new() -> Self {
        PrimToPrim {
            _s1: PhantomData,
            _s2: PhantomData,
        }
    }
}

impl<S1, S2> CastFunction for PrimToPrim<S1, S2>
where
    S1: ScalarStorage,
    S1::StorageType: ToPrimitive + Display + Sized + Copy,
    S2: MutableScalarStorage,
    S2::StorageType: NumCast + Copy,
{
    type State = ();

    fn bind(&self, _src: &DataType, _target: &DataType) -> Result<Self::State> {
        Ok(())
    }

    fn cast(
        _: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<S1, S2, _>(src, sel, OutBuffer::from_array(out)?, |&v, buf| {
            match NumCast::from(v) {
                Some(v) => buf.put(&v),
                None => {
                    error_state.set_error(|| {
                        DbError::new(format!(
                            "Failed to cast value '{}' from {} to {}",
                            v,
                            S1::PHYSICAL_TYPE,
                            S2::PHYSICAL_TYPE,
                        ))
                    });
                    buf.put_null();
                }
            }
        })?;

        error_state.into_result()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalToFloat<D, S> {
    _d: PhantomData<D>,
    _s: PhantomData<S>,
}

#[derive(Debug)]
pub struct DecimalToFloatState<F> {
    scale: F,
}

impl<D, S> DecimalToFloat<D, S> {
    pub const fn new() -> Self {
        DecimalToFloat {
            _d: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<D, S> CastFunction for DecimalToFloat<D, S>
where
    D: DecimalType,
    S: MutableScalarStorage,
    S::StorageType: Float + Copy,
{
    type State = DecimalToFloatState<S::StorageType>;

    fn bind(&self, src: &DataType, _target: &DataType) -> Result<Self::State> {
        let decimal_meta = src.try_get_decimal_type_meta()?;
        let scale = <S::StorageType as NumCast>::from((10.0).powi(decimal_meta.scale as i32))
            .ok_or_else(|| {
                DbError::new(format!(
                    "Failed to cast scale {} to float",
                    decimal_meta.scale
                ))
            })?;

        Ok(DecimalToFloatState { scale })
    }

    fn cast(
        state: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<D::Storage, S, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |&v, buf| match <S::StorageType as NumCast>::from(v) {
                Some(v) => {
                    let scaled = v / state.scale;
                    buf.put(&scaled);
                }
                None => {
                    error_state.set_error(|| DbError::new("Failed to cast decimal to float"));
                    buf.put_null();
                }
            },
        )?;

        error_state.into_result()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Utf8ToPrim<S> {
    _s: PhantomData<S>,
}

impl<S> Utf8ToPrim<S> {
    pub const fn new() -> Self {
        Utf8ToPrim { _s: PhantomData }
    }
}

impl<S> CastFunction for Utf8ToPrim<S>
where
    S: MutableScalarStorage,
    S::StorageType: FromStr,
{
    type State = ();

    fn bind(&self, _src: &DataType, _target: &DataType) -> Result<Self::State> {
        Ok(())
    }

    fn cast(
        _: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        // TODO: Temp until... not sure. Is this actually reasonable?
        let id = out.datatype.datatype_id();

        UnaryExecutor::execute::<PhysicalUtf8, S, _>(
            src,
            sel,
            OutBuffer::from_array(out)?,
            |v, buf| match v.parse() {
                Ok(v) => buf.put(&v),
                Err(_) => {
                    error_state
                        .set_error(|| DbError::new(format!("Failed to parse '{v}' into {id}")));
                    buf.put_null();
                }
            },
        )?;

        error_state.into_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DecimalTypeMeta;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::functions::cast::behavior::CastFailBehavior;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn cast_i32_to_i32() {
        let arr = Array::try_from_iter([4, 5, 6]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        let error_state = CastFailBehavior::Error.new_state();
        PrimToPrim::<PhysicalI32, PhysicalI32>::cast(&(), error_state, &arr, 0..3, &mut out)
            .unwrap();

        let expected = Array::try_from_iter([4, 5, 6]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn cast_utf8_to_i32() {
        let arr = Array::try_from_iter(["13", "18", "123456789"]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        let error_state = CastFailBehavior::Error.new_state();
        Utf8ToPrim::<PhysicalI32>::cast(&(), error_state, &arr, 0..3, &mut out).unwrap();

        let expected = Array::try_from_iter([13, 18, 123456789]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn cast_utf8_to_i32_overflow_error() {
        let arr = Array::try_from_iter(["13", "18", "123456789000000"]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        let error_state = CastFailBehavior::Error.new_state();
        Utf8ToPrim::<PhysicalI32>::cast(&(), error_state, &arr, 0..3, &mut out).unwrap_err();
    }

    #[test]
    fn cast_utf8_to_i32_overflow_null() {
        let arr = Array::try_from_iter(["13", "18", "123456789000000"]).unwrap();
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        let error_state = CastFailBehavior::Null.new_state();
        Utf8ToPrim::<PhysicalI32>::cast(&(), error_state, &arr, 0..3, &mut out).unwrap();

        let expected = Array::try_from_iter([Some(13), Some(18), None]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn array_cast_decimal64_to_f64() {
        let mut arr = Array::try_from_iter([1500_i64, 2000, 2500]).unwrap();
        // '[1.500, 2.000, 2.500]'
        arr.datatype = DataType::Decimal64(DecimalTypeMeta::new(10, 3));
        let mut out = Array::new(&NopBufferManager, DataType::Float64, 3).unwrap();

        let error_state = CastFailBehavior::Error.new_state();

        let cast = DecimalToFloat::<Decimal64Type, PhysicalF64>::new();
        let state = cast.bind(&arr.datatype, &DataType::Float64).unwrap();
        DecimalToFloat::<Decimal64Type, PhysicalF64>::cast(
            &state,
            error_state,
            &arr,
            0..3,
            &mut out,
        )
        .unwrap();

        let expected = Array::try_from_iter([1.5_f64, 2.0, 2.5]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
