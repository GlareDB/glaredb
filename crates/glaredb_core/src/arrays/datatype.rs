use std::fmt;
use std::sync::Arc;

use glaredb_error::{DbError, OptionExt, Result, ResultExt};
use glaredb_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::arrays::array::physical_type::PhysicalType;
use crate::arrays::field::Field;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};

/// The 'type' of the dataype.
///
/// This is mostly used for determining the input and return types functions
/// without needing to worry about extra type info (e.g. precision/scale for
/// decimals).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataTypeId {
    /// Any datatype.
    ///
    /// This is used for functions that can accept any input. Like all other
    /// variants, this variant must be explicitly matched on. Checking equality
    /// with any other data type will always return false.
    ///
    /// This is mostly useful for a saying a UDF can accept any type.
    Any,
    /// Represents a "table".
    ///
    /// This is useful for table functions (as they produce tables). We can't
    /// glean anything else from this type without additional binding.
    Table,
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float16,
    Float32,
    Float64,
    /// 64-bit decimal.
    Decimal64,
    /// 128-bit decimal.
    Decimal128,
    /// Timestamp, represented as an i64.
    Timestamp,
    /// Days since unix epoch.
    Date32,
    /// Milliseconds since unix epoch.
    Date64,
    /// Some time interval with nanosecond resolution.
    Interval,
    Utf8,
    Binary,
    /// A struct of different types.
    Struct,
    /// A list of values all of the same type.
    List,
}

impl fmt::Display for DataTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => write!(f, "Any"),
            Self::Table => write!(f, "Table"),
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::Int128 => write!(f, "Int128"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::UInt128 => write!(f, "UInt128"),
            Self::Float16 => write!(f, "Float16"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Decimal64 => write!(f, "Decimal64"),
            Self::Decimal128 => write!(f, "Decimal128"),
            Self::Timestamp => write!(f, "Timestamp"),
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Interval => write!(f, "Interval"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::Binary => write!(f, "Binary"),
            Self::Struct => write!(f, "Struct"),
            Self::List => write!(f, "List"),
        }
    }
}

/// Metadata associated with decimals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DecimalTypeMeta {
    /// Number of significant digits.
    pub precision: u8,
    /// Number of digits to the right of the decimal point.
    pub scale: i8,
}

impl DecimalTypeMeta {
    pub const fn new(precision: u8, scale: i8) -> Self {
        DecimalTypeMeta { precision, scale }
    }

    /// Returns the decimal type metadata that would be able to represent the
    /// given datatype.
    ///
    /// Returns None if the datatype cannot be represented as a decimal.
    pub const fn new_for_datatype_id(id: DataTypeId) -> Option<Self> {
        match id {
            DataTypeId::Boolean => Some(DecimalTypeMeta {
                precision: 1,
                scale: 0,
            }),
            DataTypeId::Int8 => {
                // [-128, 127]
                Some(DecimalTypeMeta {
                    precision: 3,
                    scale: 0,
                })
            }
            DataTypeId::Int16 => {
                // [-32_768, 32_767]
                Some(DecimalTypeMeta {
                    precision: 5,
                    scale: 0,
                })
            }
            DataTypeId::Int32 => {
                // [-2_147_483_648, 2_147_483_647]
                Some(DecimalTypeMeta {
                    precision: 10,
                    scale: 0,
                })
            }
            DataTypeId::Int64 => {
                // [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807]
                //
                // Note this will overflow a Decimal64
                Some(DecimalTypeMeta {
                    precision: 19,
                    scale: 0,
                })
            }
            DataTypeId::Int128 => {
                // [-170_141_183_460_469_231_731_687_303_715_884_105_728, 170_141_183_460_469_231_731_687_303_715_884_105_727]
                //
                // Note that the real precision should be 39, but the max
                // precision we support is 38. If we add in a Decimal256 type,
                // we should bump this up to the right precision.
                Some(DecimalTypeMeta {
                    precision: 38,
                    scale: 0,
                })
            }
            DataTypeId::UInt8 => {
                // [0, 255]
                Some(DecimalTypeMeta {
                    precision: 3,
                    scale: 0,
                })
            }
            DataTypeId::UInt16 => {
                // [0, 65_535]
                Some(DecimalTypeMeta {
                    precision: 5,
                    scale: 0,
                })
            }
            DataTypeId::UInt32 => {
                // [0, 4_294_967_295]
                Some(DecimalTypeMeta {
                    precision: 10,
                    scale: 0,
                })
            }
            DataTypeId::UInt64 => {
                // [0, 18_446_744_073_709_551_615]
                Some(DecimalTypeMeta {
                    precision: 19,
                    scale: 0,
                })
            }
            DataTypeId::UInt128 => {
                // [0, 340_282_366_920_938_463_463_374_607_431_768_211_455]
                //
                // See note for Int128
                Some(DecimalTypeMeta {
                    precision: 38,
                    scale: 0,
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimestampTypeMeta {
    pub unit: TimeUnit,
    // TODO: Optional timezone (hence no copy)
}

impl TimestampTypeMeta {
    pub const fn new(unit: TimeUnit) -> Self {
        TimestampTypeMeta { unit }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "μs",
                TimeUnit::Nanosecond => "ns",
            }
        )
    }
}

/// Metadata associated with structs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StructTypeMeta {
    pub fields: Box<[Field]>,
}

impl StructTypeMeta {
    pub fn new(fields: impl IntoIterator<Item = Field>) -> Self {
        StructTypeMeta {
            fields: fields.into_iter().collect(),
        }
    }
}

/// Metadata associated with lists.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListTypeMeta {
    pub datatype: Box<DataType>,
}

impl ListTypeMeta {
    pub fn new(element_type: DataType) -> Self {
        ListTypeMeta {
            datatype: Box::new(element_type),
        }
    }
}

/// Additional metadata that may be associated with a type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataTypeMeta {
    Decimal(DecimalTypeMeta),
    Timestamp(TimestampTypeMeta),
    Struct(StructTypeMeta),
    List(ListTypeMeta),
    None,
}

/// Supported data types.
///
/// This generally follows Arrow's type system, but is not restricted to it.
///
/// Some types may include additional metadata, which acts to refine the type
/// even further.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataType {
    /// Identifier for the type.
    pub(crate) id: DataTypeId,
    /// Additional metadata for the type.
    pub(crate) metadata: DataTypeMeta,
}

macro_rules! datatype_new_primitive {
    ($fn_name:ident, $static_const:ident) => {
        pub fn $fn_name() -> Self {
            Self::$static_const.clone()
        }
    };
}

impl DataType {
    pub const BOOLEAN: &'static Self = &Self::new_primitive(DataTypeId::Boolean);

    pub const INT8: &'static Self = &Self::new_primitive(DataTypeId::Int8);
    pub const INT16: &'static Self = &Self::new_primitive(DataTypeId::Int16);
    pub const INT32: &'static Self = &Self::new_primitive(DataTypeId::Int32);
    pub const INT64: &'static Self = &Self::new_primitive(DataTypeId::Int64);
    pub const INT128: &'static Self = &Self::new_primitive(DataTypeId::Int128);

    pub const UINT8: &'static Self = &Self::new_primitive(DataTypeId::UInt8);
    pub const UINT16: &'static Self = &Self::new_primitive(DataTypeId::UInt16);
    pub const UINT32: &'static Self = &Self::new_primitive(DataTypeId::UInt32);
    pub const UINT64: &'static Self = &Self::new_primitive(DataTypeId::UInt64);
    pub const UINT128: &'static Self = &Self::new_primitive(DataTypeId::UInt128);

    pub const FLOAT16: &'static Self = &Self::new_primitive(DataTypeId::Float16);
    pub const FLOAT32: &'static Self = &Self::new_primitive(DataTypeId::Float32);
    pub const FLOAT64: &'static Self = &Self::new_primitive(DataTypeId::Float64);

    pub const DATE32: &'static Self = &Self::new_primitive(DataTypeId::Date32);
    pub const DATE64: &'static Self = &Self::new_primitive(DataTypeId::Date64);

    pub const INTERVAL: &'static Self = &Self::new_primitive(DataTypeId::Interval);

    pub const UTF8: &'static Self = &Self::new_primitive(DataTypeId::Utf8);
    pub const BINARY: &'static Self = &Self::new_primitive(DataTypeId::Binary);

    datatype_new_primitive!(boolean, BOOLEAN);

    datatype_new_primitive!(int8, INT8);
    datatype_new_primitive!(int16, INT16);
    datatype_new_primitive!(int32, INT32);
    datatype_new_primitive!(int64, INT64);
    datatype_new_primitive!(int128, INT128);

    datatype_new_primitive!(uint8, UINT8);
    datatype_new_primitive!(uint16, UINT16);
    datatype_new_primitive!(uint32, UINT32);
    datatype_new_primitive!(uint64, UINT64);
    datatype_new_primitive!(uint128, UINT128);

    datatype_new_primitive!(float16, FLOAT16);
    datatype_new_primitive!(float32, FLOAT32);
    datatype_new_primitive!(float64, FLOAT64);

    datatype_new_primitive!(date32, DATE32);
    datatype_new_primitive!(date64, DATE64);

    datatype_new_primitive!(interval, INTERVAL);

    datatype_new_primitive!(utf8, UTF8);
    datatype_new_primitive!(binary, BINARY);

    const fn new_primitive(id: DataTypeId) -> Self {
        DataType {
            id,
            metadata: DataTypeMeta::None,
        }
    }

    pub fn list(element_type: DataType) -> Self {
        DataType {
            id: DataTypeId::List,
            metadata: DataTypeMeta::List(ListTypeMeta::new(element_type)),
        }
    }

    pub fn decimal64(m: DecimalTypeMeta) -> Self {
        DataType {
            id: DataTypeId::Decimal64,
            metadata: DataTypeMeta::Decimal(m),
        }
    }

    pub fn decimal128(m: DecimalTypeMeta) -> Self {
        DataType {
            id: DataTypeId::Decimal128,
            metadata: DataTypeMeta::Decimal(m),
        }
    }

    pub fn timestamp(m: TimestampTypeMeta) -> Self {
        DataType {
            id: DataTypeId::Timestamp,
            metadata: DataTypeMeta::Timestamp(m),
        }
    }

    /// Try to generate a datatype for casting `from` type id to `to` type id.
    ///
    /// Errors if a suitable datatype cannot be created.
    // TODO: This should probably move.
    pub fn try_generate_cast_datatype(from: DataType, to: DataTypeId) -> Result<Self> {
        let (id, meta) = match to {
            DataTypeId::Any => {
                return Err(DbError::new("Cannot cast to Any"));
            }
            DataTypeId::Table => {
                return Err(DbError::new("Cannot cast to Table"));
            }
            DataTypeId::Null => (DataTypeId::Null, DataTypeMeta::None),
            DataTypeId::Boolean => (DataTypeId::Boolean, DataTypeMeta::None),
            DataTypeId::Int8 => (DataTypeId::Int8, DataTypeMeta::None),
            DataTypeId::Int16 => (DataTypeId::Int16, DataTypeMeta::None),
            DataTypeId::Int32 => (DataTypeId::Int32, DataTypeMeta::None),
            DataTypeId::Int64 => (DataTypeId::Int64, DataTypeMeta::None),
            DataTypeId::Int128 => (DataTypeId::Int128, DataTypeMeta::None),
            DataTypeId::UInt8 => (DataTypeId::UInt8, DataTypeMeta::None),
            DataTypeId::UInt16 => (DataTypeId::UInt16, DataTypeMeta::None),
            DataTypeId::UInt32 => (DataTypeId::UInt32, DataTypeMeta::None),
            DataTypeId::UInt64 => (DataTypeId::UInt64, DataTypeMeta::None),
            DataTypeId::UInt128 => (DataTypeId::UInt128, DataTypeMeta::None),
            DataTypeId::Float16 => (DataTypeId::Float16, DataTypeMeta::None),
            DataTypeId::Float32 => (DataTypeId::Float32, DataTypeMeta::None),
            DataTypeId::Float64 => (DataTypeId::Float64, DataTypeMeta::None),
            DataTypeId::Decimal64 => match from.id {
                DataTypeId::Decimal64 => (from.id, from.metadata),
                other => {
                    let meta = DecimalTypeMeta::new_for_datatype_id(other).ok_or_else(|| {
                        DbError::new(format!(
                            "Cannot create decimal datatype for casting from {other} to {to}"
                        ))
                    })?;
                    Decimal64Type::validate_precision(0, meta.precision).context_fn(|| {
                        format!("Cannot create decimal datatype for casting from {other} to {to}")
                    })?;

                    (DataTypeId::Decimal64, DataTypeMeta::Decimal(meta))
                }
            },
            DataTypeId::Decimal128 => match from.id {
                DataTypeId::Decimal64 | DataTypeId::Decimal128 => {
                    (DataTypeId::Decimal128, from.metadata)
                }
                other => {
                    let meta = DecimalTypeMeta::new_for_datatype_id(other).ok_or_else(|| {
                        DbError::new(format!(
                            "Cannot create decimal datatype for casting from {other} to {to}"
                        ))
                    })?;
                    Decimal128Type::validate_precision(0, meta.precision).context_fn(|| {
                        format!("Cannot create decimal datatype for casting from {other} to {to}")
                    })?;

                    (DataTypeId::Decimal128, DataTypeMeta::Decimal(meta))
                }
            },
            DataTypeId::Timestamp => (
                DataTypeId::Timestamp,
                DataTypeMeta::Timestamp(TimestampTypeMeta {
                    unit: TimeUnit::Microsecond,
                }),
            ),
            DataTypeId::Date32 => (DataTypeId::Date32, DataTypeMeta::None),
            DataTypeId::Date64 => (DataTypeId::Date64, DataTypeMeta::None),
            DataTypeId::Interval => (DataTypeId::Interval, DataTypeMeta::None),
            DataTypeId::Utf8 => (DataTypeId::Utf8, DataTypeMeta::None),
            DataTypeId::Binary => (DataTypeId::Binary, DataTypeMeta::None),
            DataTypeId::Struct => {
                return Err(DbError::new("Cannot create a default Struct datatype"));
            }
            DataTypeId::List => {
                return Err(DbError::new("Cannot create a default List datatype"));
            }
        };

        Ok(DataType { id, metadata: meta })
    }

    /// Get the data type id from the data type.
    pub fn datatype_id(&self) -> DataTypeId {
        self.id
    }

    pub fn physical_type(&self) -> Result<PhysicalType> {
        Ok(match self.id {
            DataTypeId::Null => PhysicalType::UntypedNull,
            DataTypeId::Boolean => PhysicalType::Boolean,
            DataTypeId::Int8 => PhysicalType::Int8,
            DataTypeId::Int16 => PhysicalType::Int16,
            DataTypeId::Int32 => PhysicalType::Int32,
            DataTypeId::Int64 => PhysicalType::Int64,
            DataTypeId::Int128 => PhysicalType::Int128,
            DataTypeId::UInt8 => PhysicalType::UInt8,
            DataTypeId::UInt16 => PhysicalType::UInt16,
            DataTypeId::UInt32 => PhysicalType::UInt32,
            DataTypeId::UInt64 => PhysicalType::UInt64,
            DataTypeId::UInt128 => PhysicalType::UInt128,
            DataTypeId::Float16 => PhysicalType::Float16,
            DataTypeId::Float32 => PhysicalType::Float32,
            DataTypeId::Float64 => PhysicalType::Float64,
            DataTypeId::Decimal64 => PhysicalType::Int64,
            DataTypeId::Decimal128 => PhysicalType::Int128,
            DataTypeId::Timestamp => PhysicalType::Int64,
            DataTypeId::Date32 => PhysicalType::Int32,
            DataTypeId::Date64 => PhysicalType::Int64,
            DataTypeId::Interval => PhysicalType::Interval,
            DataTypeId::Utf8 => PhysicalType::Utf8,
            DataTypeId::Binary => PhysicalType::Binary,
            DataTypeId::Struct => PhysicalType::Struct,
            DataTypeId::List => PhysicalType::List,
            other => {
                return Err(DbError::new(format!(
                    "Cannot get physical type for {other}"
                )));
            }
        })
    }

    /// Return if this datatype is null.
    pub const fn is_null(&self) -> bool {
        matches!(self.id, DataTypeId::Null)
    }

    pub const fn is_utf8(&self) -> bool {
        matches!(self.id, DataTypeId::Utf8)
    }

    pub const fn is_numeric(&self) -> bool {
        matches!(
            self.id,
            DataTypeId::Int8
                | DataTypeId::Int16
                | DataTypeId::Int32
                | DataTypeId::Int64
                | DataTypeId::Int128
                | DataTypeId::UInt8
                | DataTypeId::UInt16
                | DataTypeId::UInt32
                | DataTypeId::UInt64
                | DataTypeId::UInt128
                | DataTypeId::Float32
                | DataTypeId::Float64
                | DataTypeId::Decimal64
                | DataTypeId::Decimal128
        )
    }

    pub fn try_get_decimal_type_meta(&self) -> Result<DecimalTypeMeta> {
        match &self.metadata {
            DataTypeMeta::Decimal(m) => Ok(*m),
            other => Err(DbError::new(format!(
                "Cannot get decimal type meta from metadata {other:?}"
            ))),
        }
    }

    pub fn try_get_list_type_meta(&self) -> Result<&ListTypeMeta> {
        match &self.metadata {
            DataTypeMeta::List(m) => Ok(m),
            other => Err(DbError::new(format!(
                "Cannot get list type meta from metadata {other:?}"
            ))),
        }
    }

    pub fn try_get_timestamp_type_meta(&self) -> Result<&TimestampTypeMeta> {
        match &self.metadata {
            DataTypeMeta::Timestamp(m) => Ok(m),
            other => Err(DbError::new(format!(
                "Cannot get timestamp time unit from metadata {other:?}"
            ))),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.id {
            DataTypeId::Any => write!(f, "Any"),
            DataTypeId::Table => write!(f, "Table"),
            DataTypeId::Null => write!(f, "Null"),
            DataTypeId::Boolean => write!(f, "Boolean"),
            DataTypeId::Int8 => write!(f, "Int8"),
            DataTypeId::Int16 => write!(f, "Int16"),
            DataTypeId::Int32 => write!(f, "Int32"),
            DataTypeId::Int64 => write!(f, "Int64"),
            DataTypeId::Int128 => write!(f, "Int128"),
            DataTypeId::UInt8 => write!(f, "UInt8"),
            DataTypeId::UInt16 => write!(f, "UInt16"),
            DataTypeId::UInt32 => write!(f, "UInt32"),
            DataTypeId::UInt64 => write!(f, "UInt64"),
            DataTypeId::UInt128 => write!(f, "UInt128"),
            DataTypeId::Float16 => write!(f, "Float16"),
            DataTypeId::Float32 => write!(f, "Float32"),
            DataTypeId::Float64 => write!(f, "Float64"),
            DataTypeId::Decimal64 => match &self.metadata {
                DataTypeMeta::Decimal(m) => {
                    write!(f, "Decimal64({},{})", m.precision, m.scale)
                }
                _ => write!(f, "Decimal64(Unknown,Unknown)"),
            },
            DataTypeId::Decimal128 => match &self.metadata {
                DataTypeMeta::Decimal(m) => {
                    write!(f, "Decimal128({},{})", m.precision, m.scale)
                }
                _ => write!(f, "Decimal128(Unknown,Unknown)"),
            },
            DataTypeId::Timestamp => match &self.metadata {
                DataTypeMeta::Timestamp(m) => write!(f, "Timestamp({})", m.unit),
                _ => write!(f, "Timestamp(Unknown)"),
            },
            DataTypeId::Date32 => write!(f, "Date32"),
            DataTypeId::Date64 => write!(f, "Date64"),
            DataTypeId::Interval => write!(f, "Interval"),
            DataTypeId::Utf8 => write!(f, "Utf8"),
            DataTypeId::Binary => write!(f, "Binary"),
            DataTypeId::Struct => match &self.metadata {
                DataTypeMeta::Struct(m) => {
                    write!(
                        f,
                        "Struct {{{}}}",
                        m.fields
                            .iter()
                            .map(|field| format!("{}: {}", field.name, field.datatype))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
                _ => write!(f, "Struct Unknown"),
            },
            DataTypeId::List => match &self.metadata {
                DataTypeMeta::List(m) => write!(f, "List[{}]", m.datatype),
                _ => write!(f, "List[Unknown]"),
            },
        }
    }
}
