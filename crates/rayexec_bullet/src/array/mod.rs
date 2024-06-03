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
use rayexec_error::{RayexecError, Result};
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        match self {
            Self::Null(arr) => Some(arr.validity()),
            Self::Boolean(arr) => arr.validity(),
            Self::Float32(arr) => arr.validity(),
            Self::Float64(arr) => arr.validity(),
            Self::Int8(arr) => arr.validity(),
            Self::Int16(arr) => arr.validity(),
            Self::Int32(arr) => arr.validity(),
            Self::Int64(arr) => arr.validity(),
            Self::UInt8(arr) => arr.validity(),
            Self::UInt16(arr) => arr.validity(),
            Self::UInt32(arr) => arr.validity(),
            Self::UInt64(arr) => arr.validity(),
            Self::Utf8(arr) => arr.validity(),
            Self::LargeUtf8(arr) => arr.validity(),
            Self::Binary(arr) => arr.validity(),
            Self::LargeBinary(arr) => arr.validity(),
            Self::Struct(_arr) => unimplemented!(),
        }
    }

    /// Try to convert an iterator of scalars of a given datatype into an array.
    ///
    /// Errors if any of the scalars are a different type than the provided
    /// datatype.
    pub fn try_from_scalars<'a>(
        datatype: DataType,
        scalars: impl Iterator<Item = ScalarValue<'a>>,
    ) -> Result<Array> {
        /// Helper for iterating over scalars and producing a single type of
        /// array.
        ///
        /// `builder` is the array builder we're pushing values to.
        ///
        /// `default` is the default value to use if the we want to push a "null".
        ///
        /// `variant` is the enum variant for the Array and ScalarValue.
        macro_rules! iter_scalars_for_type {
            ($builder:expr, $default:expr, $variant:ident) => {{
                let mut validity = Bitmap::default();
                let mut builder = $builder;
                for scalar in scalars {
                    match scalar {
                        ScalarValue::Null => {
                            validity.push(false);
                            builder.push_value($default);
                        }
                        ScalarValue::$variant(v) => {
                            validity.push(true);
                            builder.push_value(v);
                        }
                        other => {
                            return Err(RayexecError::new(format!(
                                "Unexpected scalar value: {other}"
                            )))
                        }
                    }
                }
                builder.put_validity(validity);
                Ok(Array::$variant(builder.into_typed_array()))
            }};
        }

        let (cap, _) = scalars.size_hint();

        match datatype {
            DataType::Null => {
                let mut len = 0;
                for scalar in scalars {
                    match scalar {
                        ScalarValue::Null => len += 1,
                        other => {
                            return Err(RayexecError::new(format!(
                                "Unexpected non-null scalar: {other}"
                            )))
                        }
                    }
                }
                Ok(Array::Null(NullArray::new(len)))
            }
            DataType::Boolean => iter_scalars_for_type!(BooleanArrayBuilder::new(), false, Boolean),
            DataType::Float32 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0.0, Float32)
            }
            DataType::Float64 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0.0, Float64)
            }
            DataType::Int8 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, Int8)
            }
            DataType::Int16 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, Int16)
            }
            DataType::Int32 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, Int32)
            }
            DataType::Int64 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, Int64)
            }
            DataType::UInt8 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, UInt8)
            }
            DataType::UInt16 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, UInt16)
            }
            DataType::UInt32 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, UInt32)
            }
            DataType::UInt64 => {
                iter_scalars_for_type!(PrimitiveArrayBuilder::with_capacity(cap), 0, UInt64)
            }
            DataType::Utf8 => {
                iter_scalars_for_type!(VarlenArrayBuilder::new(), "".into(), Utf8)
            }
            DataType::LargeUtf8 => {
                iter_scalars_for_type!(VarlenArrayBuilder::new(), "".into(), LargeUtf8)
            }
            DataType::Binary => {
                iter_scalars_for_type!(VarlenArrayBuilder::new(), (&[]).into(), Binary)
            }
            DataType::LargeBinary => {
                iter_scalars_for_type!(VarlenArrayBuilder::new(), (&[]).into(), LargeBinary)
            }
            DataType::Struct { .. } => Err(RayexecError::new(
                "Cannot build a struct array from struct scalars",
            )), // yet
        }
    }
}

/// Utility trait for iterating over arrays.
pub trait ArrayAccessor<T: ?Sized> {
    type ValueIter: Iterator<Item = T>;

    /// Return the length of the array.
    fn len(&self) -> usize;

    /// If this array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

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

/// An implementation of an accessor that just returns unit values for
/// everything.
///
/// This is useful for when we care about iterating over arrays, but don't care
/// about the actual values. The primary use case for this is COUNT, as it
/// doesn't care about its input, other than if it's null which the validity
/// bitmap provides us.
pub struct UnitArrayAccessor<'a> {
    inner: &'a Array,
}

impl<'a> UnitArrayAccessor<'a> {
    pub fn new(arr: &'a Array) -> Self {
        UnitArrayAccessor { inner: arr }
    }
}

impl<'a> ArrayAccessor<()> for UnitArrayAccessor<'a> {
    type ValueIter = UnitIterator;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn values_iter(&self) -> Self::ValueIter {
        UnitIterator {
            idx: 0,
            len: self.inner.len(),
        }
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.inner.validity()
    }
}

#[derive(Debug)]
pub struct UnitIterator {
    idx: usize,
    len: usize,
}

impl Iterator for UnitIterator {
    type Item = ();
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.len {
            None
        } else {
            Some(())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.idx;
        (rem, Some(rem))
    }
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
