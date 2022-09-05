use crate::errors::{LemurError, Result};
use arrow2::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

/// A trait that has a type that's convertible to an arrow data type.
pub trait GetArrowDataType {
    fn get_arrow_data_type(&self) -> ArrowDataType;
}

/// Supported datatypes for the database.
///
/// These data types are a subset of data types supported by arrow. Logical
/// mappings between this and the equivalent arrow data type must use the same
/// underlying physical type. For example, a `Date32` must use i32 as the
/// primitive, not u32.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float32,
    Float64,
    Binary,
    Utf8,
    Date32,
    Date64,
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = LemurError;
    fn try_from(value: &ArrowDataType) -> Result<Self> {
        Ok(match value {
            ArrowDataType::Null => DataType::Null,
            ArrowDataType::Boolean => DataType::Bool,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::UInt8 => DataType::Uint8,
            ArrowDataType::UInt16 => DataType::Uint16,
            ArrowDataType::UInt32 => DataType::Uint32,
            ArrowDataType::UInt64 => DataType::Uint64,
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Binary => DataType::Binary,
            ArrowDataType::Utf8 => DataType::Utf8,
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,
            other => return Err(LemurError::UnsupportedArrowDataType(other.clone())),
        })
    }
}

impl From<DataType> for ArrowDataType {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Null => ArrowDataType::Null,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Uint8 => ArrowDataType::UInt8,
            DataType::Uint16 => ArrowDataType::UInt16,
            DataType::Uint32 => ArrowDataType::UInt32,
            DataType::Uint64 => ArrowDataType::UInt64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Binary => ArrowDataType::Binary,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
        }
    }
}
