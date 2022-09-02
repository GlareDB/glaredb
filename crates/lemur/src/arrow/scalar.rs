use crate::arrow::datatype::{DataType, GetArrowDataType};
use crate::errors::{LemurError, Result};
use arrow2::datatypes::DataType as ArrowDataType;
use arrow2::scalar::Scalar as ArrowScalar;
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
    Date32(Option<u32>),
    Date64(Option<u64>),
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

    pub fn cast(&self, _dt: DataType) -> Result<Self> {
        todo!()
    }
}

impl GetArrowDataType for ScalarOwned {
    fn get_arrow_data_type(&self) -> ArrowDataType {
        self.data_type().into()
    }
}
