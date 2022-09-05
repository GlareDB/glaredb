use crate::arrow::datatype::{DataType, GetArrowDataType};
use crate::errors::{internal, LemurError, Result};
use arrow2::datatypes::DataType as ArrowDataType;
use arrow2::scalar::{
    BinaryScalar, BooleanScalar, PrimitiveScalar, Scalar as ArrowScalar, Utf8Scalar,
};
use serde::{Deserialize, Serialize};

/// A scalar value that owns all of it's data.
// TODO: Custom partial eq/ord
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarOwned {
    Null,
    Bool(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Uint8(Option<u8>),
    Uint16(Option<u16>),
    Uint32(Option<u32>),
    Uint64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Binary(Option<Vec<u8>>),
    Utf8(Option<String>),
    Date32(Option<i32>),
    Date64(Option<i64>),
}

impl ScalarOwned {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarOwned::Null => DataType::Null,
            ScalarOwned::Bool(_) => DataType::Bool,
            ScalarOwned::Int8(_) => DataType::Int8,
            ScalarOwned::Int16(_) => DataType::Int16,
            ScalarOwned::Int32(_) => DataType::Int32,
            ScalarOwned::Int64(_) => DataType::Int64,
            ScalarOwned::Uint8(_) => DataType::Uint8,
            ScalarOwned::Uint16(_) => DataType::Uint16,
            ScalarOwned::Uint32(_) => DataType::Uint32,
            ScalarOwned::Uint64(_) => DataType::Uint64,
            ScalarOwned::Float32(_) => DataType::Float32,
            ScalarOwned::Float64(_) => DataType::Float64,
            ScalarOwned::Binary(_) => DataType::Binary,
            ScalarOwned::Utf8(_) => DataType::Utf8,
            ScalarOwned::Date32(_) => DataType::Date32,
            ScalarOwned::Date64(_) => DataType::Date64,
        }
    }

    pub fn new_null_with_type(dt: DataType) -> ScalarOwned {
        match dt {
            DataType::Null => ScalarOwned::Null,
            DataType::Bool => ScalarOwned::Bool(None),
            DataType::Int8 => ScalarOwned::Int8(None),
            DataType::Int16 => ScalarOwned::Int16(None),
            DataType::Int32 => ScalarOwned::Int32(None),
            DataType::Int64 => ScalarOwned::Int64(None),
            DataType::Uint8 => ScalarOwned::Uint8(None),
            DataType::Uint16 => ScalarOwned::Uint16(None),
            DataType::Uint32 => ScalarOwned::Uint32(None),
            DataType::Uint64 => ScalarOwned::Uint64(None),
            DataType::Float32 => ScalarOwned::Float32(None),
            DataType::Float64 => ScalarOwned::Float64(None),
            DataType::Binary => ScalarOwned::Binary(None),
            DataType::Utf8 => ScalarOwned::Utf8(None),
            DataType::Date32 => ScalarOwned::Date32(None),
            DataType::Date64 => ScalarOwned::Date64(None),
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarOwned::Null
            | ScalarOwned::Bool(None)
            | ScalarOwned::Int8(None)
            | ScalarOwned::Int16(None)
            | ScalarOwned::Int32(None)
            | ScalarOwned::Int64(None)
            | ScalarOwned::Uint8(None)
            | ScalarOwned::Uint16(None)
            | ScalarOwned::Uint32(None)
            | ScalarOwned::Uint64(None)
            | ScalarOwned::Float32(None)
            | ScalarOwned::Float64(None)
            | ScalarOwned::Binary(None)
            | ScalarOwned::Utf8(None)
            | ScalarOwned::Date32(None)
            | ScalarOwned::Date64(None) => true,
            _ => false,
        }
    }

    pub fn unwrap_bool(self) -> Result<Option<bool>, Self> {
        match self {
            ScalarOwned::Bool(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_int8(self) -> Result<Option<i8>, Self> {
        match self {
            ScalarOwned::Int8(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_int16(self) -> Result<Option<i16>, Self> {
        match self {
            ScalarOwned::Int16(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_int32(self) -> Result<Option<i32>, Self> {
        match self {
            ScalarOwned::Int32(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_int64(self) -> Result<Option<i64>, Self> {
        match self {
            ScalarOwned::Int64(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_uint8(self) -> Result<Option<u8>, Self> {
        match self {
            ScalarOwned::Uint8(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_uint16(self) -> Result<Option<u16>, Self> {
        match self {
            ScalarOwned::Uint16(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_uint32(self) -> Result<Option<u32>, Self> {
        match self {
            ScalarOwned::Uint32(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_uint64(self) -> Result<Option<u64>, Self> {
        match self {
            ScalarOwned::Uint64(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_float32(self) -> Result<Option<f32>, Self> {
        match self {
            ScalarOwned::Float32(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_float64(self) -> Result<Option<f64>, Self> {
        match self {
            ScalarOwned::Float64(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_binary(self) -> Result<Option<Vec<u8>>, Self> {
        match self {
            ScalarOwned::Binary(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_utf8(self) -> Result<Option<String>, Self> {
        match self {
            ScalarOwned::Utf8(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_date32(self) -> Result<Option<i32>, Self> {
        match self {
            ScalarOwned::Date32(v) => Ok(v),
            other => Err(other),
        }
    }

    pub fn unwrap_date64(self) -> Result<Option<i64>, Self> {
        match self {
            ScalarOwned::Date64(v) => Ok(v),
            other => Err(other),
        }
    }
}

impl GetArrowDataType for ScalarOwned {
    fn get_arrow_data_type(&self) -> ArrowDataType {
        self.data_type().into()
    }
}

impl TryFrom<Box<dyn ArrowScalar>> for ScalarOwned {
    type Error = LemurError;
    fn try_from(value: Box<dyn ArrowScalar>) -> Result<Self> {
        Ok(match value.data_type() {
            ArrowDataType::Null => ScalarOwned::Null,
            ArrowDataType::Boolean => ScalarOwned::Bool(
                value
                    .as_any()
                    .downcast_ref::<BooleanScalar>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Int8 => ScalarOwned::Int8(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i8>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Int16 => ScalarOwned::Int16(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i16>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Int32 => ScalarOwned::Int32(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i32>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Int64 => ScalarOwned::Int64(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i64>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::UInt8 => ScalarOwned::Uint8(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<u8>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::UInt16 => ScalarOwned::Uint16(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<u16>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::UInt32 => ScalarOwned::Uint32(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<u32>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::UInt64 => ScalarOwned::Uint64(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<u64>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Float32 => ScalarOwned::Float32(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<f32>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Float64 => ScalarOwned::Float64(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<f64>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Binary => ScalarOwned::Binary(
                value
                    .as_any()
                    .downcast_ref::<BinaryScalar<i32>>()
                    .unwrap()
                    .value()
                    .map(|s| s.to_vec()),
            ),
            ArrowDataType::Utf8 => ScalarOwned::Utf8(
                value
                    .as_any()
                    .downcast_ref::<Utf8Scalar<i32>>()
                    .unwrap()
                    .value()
                    .map(|s| s.to_string()),
            ),
            ArrowDataType::Date32 => ScalarOwned::Date32(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i32>>()
                    .unwrap()
                    .value(),
            ),
            ArrowDataType::Date64 => ScalarOwned::Int64(
                *value
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i64>>()
                    .unwrap()
                    .value(),
            ),
            other => return Err(internal!("unsupported arrow data type: {:?}", other)),
        })
    }
}
