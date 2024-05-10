pub mod null;
pub use null::*;
pub mod boolean;
pub mod struct_array;
pub use boolean::*;
pub use struct_array::*;
pub mod primitive;
pub use primitive::*;
pub mod varlen;
pub use varlen::*;

use crate::bitmap::Bitmap;
use crate::field::DataType;
use crate::scalar::ScalarValue;
use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum Array {
    Null(NullArray),
    Boolean(BooleanArray),
    Float32(Float32Array),
    Float64(Float64Array),
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Utf8(Utf8Array),
    LargeUtf8(LargeUtf8Array),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
    Struct(StructArray),
}

impl Array {
    pub fn datatype(&self) -> DataType {
        match self {
            Array::Null(_) => DataType::Null,
            Array::Boolean(_) => DataType::Boolean,
            Array::Float32(_) => DataType::Float32,
            Array::Float64(_) => DataType::Float64,
            Array::Int8(_) => DataType::Int8,
            Array::Int16(_) => DataType::Int16,
            Array::Int32(_) => DataType::Int32,
            Array::Int64(_) => DataType::Int64,
            Array::UInt8(_) => DataType::UInt8,
            Array::UInt16(_) => DataType::UInt16,
            Array::UInt32(_) => DataType::UInt32,
            Array::UInt64(_) => DataType::UInt64,
            Array::Utf8(_) => DataType::Utf8,
            Array::LargeUtf8(_) => DataType::LargeUtf8,
            Array::Binary(_) => DataType::Binary,
            Array::LargeBinary(_) => DataType::LargeBinary,
            Self::Struct(arr) => arr.datatype(),
        }
    }

    /// Get a scalar value at the given index.
    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if !self.is_valid(idx)? {
            return Some(ScalarValue::Null);
        }

        Some(match self {
            Self::Null(_) => panic!("nulls should be handled by validity check"),
            Self::Boolean(arr) => ScalarValue::Boolean(arr.value(idx)?),
            Self::Float32(arr) => ScalarValue::Float32(*arr.value(idx)?),
            Self::Float64(arr) => ScalarValue::Float64(*arr.value(idx)?),
            Self::Int8(arr) => ScalarValue::Int8(*arr.value(idx)?),
            Self::Int16(arr) => ScalarValue::Int16(*arr.value(idx)?),
            Self::Int32(arr) => ScalarValue::Int32(*arr.value(idx)?),
            Self::Int64(arr) => ScalarValue::Int64(*arr.value(idx)?),
            Self::UInt8(arr) => ScalarValue::UInt8(*arr.value(idx)?),
            Self::UInt16(arr) => ScalarValue::UInt16(*arr.value(idx)?),
            Self::UInt32(arr) => ScalarValue::UInt32(*arr.value(idx)?),
            Self::UInt64(arr) => ScalarValue::UInt64(*arr.value(idx)?),
            Self::Utf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::LargeUtf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::Binary(arr) => ScalarValue::Binary(arr.value(idx)?.into()),
            Self::LargeBinary(arr) => ScalarValue::LargeBinary(arr.value(idx)?.into()),
            Self::Struct(arr) => arr.scalar(idx)?,
        })
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        match self {
            Self::Null(arr) => arr.is_valid(idx),
            Self::Boolean(arr) => arr.is_valid(idx),
            Self::Float32(arr) => arr.is_valid(idx),
            Self::Float64(arr) => arr.is_valid(idx),
            Self::Int8(arr) => arr.is_valid(idx),
            Self::Int16(arr) => arr.is_valid(idx),
            Self::Int32(arr) => arr.is_valid(idx),
            Self::Int64(arr) => arr.is_valid(idx),
            Self::UInt8(arr) => arr.is_valid(idx),
            Self::UInt16(arr) => arr.is_valid(idx),
            Self::UInt32(arr) => arr.is_valid(idx),
            Self::UInt64(arr) => arr.is_valid(idx),
            Self::Utf8(arr) => arr.is_valid(idx),
            Self::LargeUtf8(arr) => arr.is_valid(idx),
            Self::Binary(arr) => arr.is_valid(idx),
            Self::LargeBinary(arr) => arr.is_valid(idx),
            Self::Struct(arr) => arr.is_valid(idx),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Null(arr) => arr.len(),
            Self::Boolean(arr) => arr.len(),
            Self::Float32(arr) => arr.len(),
            Self::Float64(arr) => arr.len(),
            Self::Int8(arr) => arr.len(),
            Self::Int16(arr) => arr.len(),
            Self::Int32(arr) => arr.len(),
            Self::Int64(arr) => arr.len(),
            Self::UInt8(arr) => arr.len(),
            Self::UInt16(arr) => arr.len(),
            Self::UInt32(arr) => arr.len(),
            Self::UInt64(arr) => arr.len(),
            Self::Utf8(arr) => arr.len(),
            Self::LargeUtf8(arr) => arr.len(),
            Self::Binary(arr) => arr.len(),
            Self::LargeBinary(arr) => arr.len(),
            Self::Struct(arr) => arr.len(),
        }
    }
}

/// Utility trait for iterating over arrays.
pub trait ArrayAccessor<T: ?Sized> {
    type ValueIter: Iterator<Item = T>;

    /// Return the length of the array.
    fn len(&self) -> usize;

    /// Return an iterator over the values in the array.
    ///
    /// This should iterate over values even if the validity of the value is
    /// false.
    fn values_iter(&self) -> Self::ValueIter;

    /// Return a reference to the validity bitmap if this array has one.
    fn validity(&self) -> Option<&Bitmap>;
}

/// Utility trait for building up a new array.
pub trait ArrayBuilder<T: ?Sized> {
    /// Push a value onto the builder.
    fn push_value(&mut self, value: T);

    /// Put a validity bitmap on the array.
    fn put_validity(&mut self, validity: Bitmap);
}

/// Helper for determining if a value at a given index should be considered
/// valid.
///
/// If the bitmap is None, it's assumed that all values, regardless of the
/// index, are valid.
///
/// Panics if index is out of bounds.
fn is_valid(validity: Option<&Bitmap>, idx: usize) -> bool {
    validity.map(|bm| bm.value(idx)).unwrap_or(true)
}
