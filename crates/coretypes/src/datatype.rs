use crate::totalfloat::{NotNanF32, NotNanF64};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum DataTypeError {
    #[error("invalid schema projection: attempted to project non-existend column index {missing}")]
    InvalidSchemaProjection { missing: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// A 64-bit unix timestamp in milliseconds.
    Date64,
    /// Utf-8 strings. No other string types are supported.
    Utf8,
    /// Opaque binary blob.
    Binary,
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        use DataType::*;
        match self {
            Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Date64 => true,
            _ => false,
        }
    }
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
            Date64 => write!(f, "DATE64"),
            Utf8 => write!(f, "UTF8"),
            Binary => write!(f, "BINARY"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NullableType {
    pub datatype: DataType,
    pub nullable: bool,
}

impl NullableType {
    pub fn new_nullable(datatype: DataType) -> NullableType {
        NullableType {
            datatype,
            nullable: true,
        }
    }

    pub fn is_numeric(&self) -> bool {
        self.datatype.is_numeric()
    }
}

impl From<DataType> for NullableType {
    fn from(dt: DataType) -> Self {
        Self::new_nullable(dt)
    }
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataValue {
    /// Unknown value.
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(NotNanF32),
    Float64(NotNanF64),
    Date64(i64), // TODO: Change this to u64.
    Utf8(String),
    Binary(Vec<u8>),
}

impl DataValue {
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
            (Date64(_), DataType::Date64) => true,
            (Utf8(_), DataType::Utf8) => true,
            (Binary(_), DataType::Binary) => true,
            _ => false,
        }
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Bool(v) => write!(f, "{}", v),
            DataValue::Int8(v) => write!(f, "{}", v),
            DataValue::Int16(v) => write!(f, "{}", v),
            DataValue::Int32(v) => write!(f, "{}", v),
            DataValue::Int64(v) => write!(f, "{}", v),
            DataValue::Float32(v) => write!(f, "{}", v),
            DataValue::Float64(v) => write!(f, "{}", v),
            DataValue::Date64(v) => write!(f, "{}", v),
            DataValue::Utf8(v) => write!(f, "{}", v),
            DataValue::Binary(v) => {
                write!(f, "hex:")?;
                for byte in v.iter() {
                    write!(f, " {:X}", byte)?;
                }
                Ok(())
            }
        }
    }
}

impl From<bool> for DataValue {
    fn from(val: bool) -> Self {
        DataValue::Bool(val)
    }
}

impl From<i8> for DataValue {
    fn from(val: i8) -> Self {
        DataValue::Int8(val)
    }
}

impl From<i16> for DataValue {
    fn from(val: i16) -> Self {
        DataValue::Int16(val)
    }
}

impl From<i32> for DataValue {
    fn from(val: i32) -> Self {
        DataValue::Int32(val)
    }
}

impl From<i64> for DataValue {
    fn from(val: i64) -> Self {
        DataValue::Int64(val)
    }
}

impl From<f32> for DataValue {
    fn from(val: f32) -> Self {
        // TODO: Properly handle this.
        DataValue::Float32(val.try_into().unwrap())
    }
}

impl From<f64> for DataValue {
    fn from(val: f64) -> Self {
        // TODO: Properly handle this.
        DataValue::Float64(val.try_into().unwrap())
    }
}

impl From<String> for DataValue {
    fn from(val: String) -> Self {
        DataValue::Utf8(val)
    }
}

impl From<Vec<u8>> for DataValue {
    fn from(val: Vec<u8>) -> Self {
        DataValue::Binary(val)
    }
}

/// Describes the schema of a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationSchema {
    pub columns: Vec<NullableType>,
}

impl RelationSchema {
    pub fn empty() -> RelationSchema {
        RelationSchema {
            columns: Vec::new(),
        }
    }

    /// Create a new schema from a list of columns.
    pub fn new(columns: Vec<NullableType>) -> RelationSchema {
        RelationSchema { columns }
    }

    /// Return a new schema containing only the columns in `indices`.
    pub fn with_projection(&self, indices: &[usize]) -> Result<RelationSchema, DataTypeError> {
        let mut columns = Vec::with_capacity(indices.len());
        for &idx in indices {
            let proj = self
                .columns
                .get(idx)
                .cloned()
                .ok_or(DataTypeError::InvalidSchemaProjection { missing: idx })?;
            columns.push(proj);
        }
        Ok(RelationSchema { columns })
    }

    /// The number of columns in the relation.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }
}
