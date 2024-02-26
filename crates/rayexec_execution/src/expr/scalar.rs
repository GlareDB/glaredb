use arrow_array::{
    new_null_array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray,
};
use arrow_schema::DataType;
use rayexec_error::Result;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
    /// true or false value
    Boolean(bool),
    /// 32bit float
    Float32(f32),
    /// 64bit float
    Float64(f64),
    // /// 128bit decimal, using the i128 to represent the decimal, precision scale
    // Decimal128(Option<i128>, u8, i8),
    // /// 256bit decimal, using the i256 to represent the decimal, precision scale
    // Decimal256(Option<i256>, u8, i8),
    /// signed 8bit int
    Int8(i8),
    /// signed 16bit int
    Int16(i16),
    /// signed 32bit int
    Int32(i32),
    /// signed 64bit int
    Int64(i64),
    /// unsigned 8bit int
    UInt8(u8),
    /// unsigned 16bit int
    UInt16(u16),
    /// unsigned 32bit int
    UInt32(u32),
    /// unsigned 64bit int
    UInt64(u64),
    /// utf-8 encoded string.
    Utf8(String),
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(String),
    /// binary
    Binary(Vec<u8>),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            // ScalarValue::Decimal128(_, precision, scale) => {
            //     DataType::Decimal128(*precision, *scale)
            // }
            // ScalarValue::Decimal256(_, precision, scale) => {
            //     DataType::Decimal256(*precision, *scale)
            // }
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
        }
    }

    /// Create an array of size `len` from the scalar value.
    pub fn as_array(&self, len: usize) -> Result<ArrayRef> {
        Ok(match self {
            Self::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; len])),
            Self::Int8(v) => Arc::new(Int8Array::from(vec![*v; len])),
            Self::Int16(v) => Arc::new(Int16Array::from(vec![*v; len])),
            Self::Int32(v) => Arc::new(Int32Array::from(vec![*v; len])),
            Self::Int64(v) => Arc::new(Int64Array::from(vec![*v; len])),
            Self::Utf8(v) => Arc::new(StringArray::from_iter_values(
                std::iter::repeat(v).take(len),
            )),
            _ => unimplemented!(),
        })
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Actual display impl
        write!(f, "{self:?}")
    }
}