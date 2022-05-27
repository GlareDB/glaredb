use crate::totalfloat::{NotNanF32, NotNanF64};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DataType::*;
        match self {
            Bool => write!(f, "BOOL"),
            Int8 => write!(f, "INT8"),
            Int16 => write!(f, "INT16"),
            Int32 => write!(f, "INT32"),
            Int64 => write!(f, "INT64"),
            Float32 => write!(f, "FLOAT32"),
            Float64 => write!(f, "FLOAT64"),
            Utf8 => write!(f, "UTF8"),
            Binary => write!(f, "BINARY"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableType {
    pub datatype: DataType,
    pub nullable: bool,
}

impl fmt::Display for NullableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nullable {
            write!(f, "{}", self.datatype)
        } else {
            write!(f, "{}?", self.datatype)
        }
    }
}

/// Possible data values that the system works with.
#[derive(Debug, Clone, PartialEq)]
pub enum DataValue<'a> {
    /// Unknown value.
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(NotNanF32),
    Float64(NotNanF64),
    Utf8(&'a str),
    Binary(&'a [u8]),
}

impl<'a> DataValue<'a> {
    /// Checks if the value is of the the given `type`.
    pub fn is_of_type(&self, typ: &NullableType) -> bool {
        use DataValue::*;
        match (self, &typ.datatype) {
            (Null, _) => typ.nullable,
            (Bool(_), DataType::Bool) => true,
            (Int8(_), DataType::Int8) => true,
            (Int16(_), DataType::Int16) => true,
            (Int32(_), DataType::Int32) => true,
            (Int64(_), DataType::Int64) => true,
            (Float32(_), DataType::Float32) => true,
            (Float64(_), DataType::Float64) => true,
            (Utf8(_), DataType::Utf8) => true,
            (Binary(_), DataType::Binary) => true,
            _ => false,
        }
    }
}

impl From<bool> for DataValue<'_> {
    fn from(val: bool) -> Self {
        DataValue::Bool(val)
    }
}

impl From<i8> for DataValue<'_> {
    fn from(val: i8) -> Self {
        DataValue::Int8(val)
    }
}

impl From<i16> for DataValue<'_> {
    fn from(val: i16) -> Self {
        DataValue::Int16(val)
    }
}

impl From<i32> for DataValue<'_> {
    fn from(val: i32) -> Self {
        DataValue::Int32(val)
    }
}

impl From<i64> for DataValue<'_> {
    fn from(val: i64) -> Self {
        DataValue::Int64(val)
    }
}

impl From<f32> for DataValue<'_> {
    fn from(val: f32) -> Self {
        // TODO: Properly handle this.
        DataValue::Float32(val.try_into().unwrap())
    }
}

impl From<f64> for DataValue<'_> {
    fn from(val: f64) -> Self {
        // TODO: Properly handle this.
        DataValue::Float64(val.try_into().unwrap())
    }
}

impl<'a> From<&'a str> for DataValue<'a> {
    fn from(val: &'a str) -> Self {
        DataValue::Utf8(val)
    }
}

impl<'a> From<&'a [u8]> for DataValue<'a> {
    fn from(val: &'a [u8]) -> Self {
        DataValue::Binary(val)
    }
}
