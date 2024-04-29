use crate::field::DataType;
use std::borrow::Cow;

/// A single scalar value.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue<'a> {
    /// Represents `DataType::Null` (castable to/from any other type)
    Null,

    /// True or false value
    Boolean(bool),

    /// 32bit float
    Float32(f32),

    /// 64bit float
    Float64(f64),

    /// Signed 8bit int
    Int8(i8),

    /// Signed 16bit int
    Int16(i16),

    /// Signed 32bit int
    Int32(i32),

    /// Signed 64bit int
    Int64(i64),

    /// Unsigned 8bit int
    UInt8(u8),

    /// Unsigned 16bit int
    UInt16(u16),

    /// Unsigned 32bit int
    UInt32(u32),

    /// Unsigned 64bit int
    UInt64(u64),

    /// Utf-8 encoded string.
    Utf8(Cow<'a, str>),

    /// Utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Cow<'a, str>),

    /// Binary
    Binary(Cow<'a, [u8]>),

    /// Large binary
    LargeBinary(Cow<'a, [u8]>),
}

pub type OwnedScalarValue = ScalarValue<'static>;

impl<'a> ScalarValue<'a> {
    pub fn datatype(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
        }
    }

    pub fn into_owned(self) -> OwnedScalarValue {
        match self {
            Self::Null => OwnedScalarValue::Null,
            Self::Boolean(v) => OwnedScalarValue::Boolean(v),
            Self::Float32(v) => OwnedScalarValue::Float32(v),
            Self::Float64(v) => OwnedScalarValue::Float64(v),
            Self::Int8(v) => OwnedScalarValue::Int8(v),
            Self::Int16(v) => OwnedScalarValue::Int16(v),
            Self::Int32(v) => OwnedScalarValue::Int32(v),
            Self::Int64(v) => OwnedScalarValue::Int64(v),
            Self::UInt8(v) => OwnedScalarValue::UInt8(v),
            Self::UInt16(v) => OwnedScalarValue::UInt16(v),
            Self::UInt32(v) => OwnedScalarValue::UInt32(v),
            Self::UInt64(v) => OwnedScalarValue::UInt64(v),
            Self::Utf8(v) => OwnedScalarValue::Utf8(v.into_owned().into()),
            Self::LargeUtf8(v) => OwnedScalarValue::LargeUtf8(v.into_owned().into()),
            Self::Binary(v) => OwnedScalarValue::Binary(v.into_owned().into()),
            Self::LargeBinary(v) => OwnedScalarValue::LargeBinary(v.into_owned().into()),
        }
    }
}

impl<'a> From<bool> for ScalarValue<'a> {
    fn from(value: bool) -> Self {
        ScalarValue::Boolean(value)
    }
}

impl<'a> From<f32> for ScalarValue<'a> {
    fn from(value: f32) -> Self {
        ScalarValue::Float32(value)
    }
}

impl<'a> From<f64> for ScalarValue<'a> {
    fn from(value: f64) -> Self {
        ScalarValue::Float64(value)
    }
}

impl<'a> From<i8> for ScalarValue<'a> {
    fn from(value: i8) -> Self {
        ScalarValue::Int8(value)
    }
}

impl<'a> From<i16> for ScalarValue<'a> {
    fn from(value: i16) -> Self {
        ScalarValue::Int16(value)
    }
}

impl<'a> From<i32> for ScalarValue<'a> {
    fn from(value: i32) -> Self {
        ScalarValue::Int32(value)
    }
}

impl<'a> From<i64> for ScalarValue<'a> {
    fn from(value: i64) -> Self {
        ScalarValue::Int64(value)
    }
}

impl<'a> From<u8> for ScalarValue<'a> {
    fn from(value: u8) -> Self {
        ScalarValue::UInt8(value)
    }
}

impl<'a> From<u16> for ScalarValue<'a> {
    fn from(value: u16) -> Self {
        ScalarValue::UInt16(value)
    }
}

impl<'a> From<u32> for ScalarValue<'a> {
    fn from(value: u32) -> Self {
        ScalarValue::UInt32(value)
    }
}

impl<'a> From<u64> for ScalarValue<'a> {
    fn from(value: u64) -> Self {
        ScalarValue::UInt64(value)
    }
}

impl<'a> From<&'a str> for ScalarValue<'a> {
    fn from(value: &'a str) -> Self {
        ScalarValue::Utf8(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a [u8]> for ScalarValue<'a> {
    fn from(value: &'a [u8]) -> Self {
        ScalarValue::Binary(Cow::Borrowed(value))
    }
}

impl<'a, T: Into<ScalarValue<'a>>> From<Option<T>> for ScalarValue<'a> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => ScalarValue::Null,
        }
    }
}
