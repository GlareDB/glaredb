use std::fmt;

use rayexec_error::{not_implemented, OptionExt, RayexecError, Result, ResultExt};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::executor::physical_type::PhysicalType;
use crate::field::Field;
use crate::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};

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
    Decimal64,
    Decimal128,
    Timestamp,
    Date32,
    Date64,
    Interval,
    Utf8,
    Binary,
    Struct,
    List,
}

impl ProtoConv for DataTypeId {
    type ProtoType = rayexec_proto::generated::schema::DataTypeId;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::Any => Self::ProtoType::Any,
            Self::Null => Self::ProtoType::Null,
            Self::Boolean => Self::ProtoType::Boolean,
            Self::Int8 => Self::ProtoType::Int8,
            Self::Int16 => Self::ProtoType::Int16,
            Self::Int32 => Self::ProtoType::Int32,
            Self::Int64 => Self::ProtoType::Int64,
            Self::Int128 => Self::ProtoType::Int128,
            Self::UInt8 => Self::ProtoType::Uint8,
            Self::UInt16 => Self::ProtoType::Uint16,
            Self::UInt32 => Self::ProtoType::Uint32,
            Self::UInt64 => Self::ProtoType::Uint64,
            Self::UInt128 => Self::ProtoType::Uint128,
            Self::Float16 => Self::ProtoType::Float16,
            Self::Float32 => Self::ProtoType::Float32,
            Self::Float64 => Self::ProtoType::Float64,
            Self::Decimal64 => Self::ProtoType::Decimal64,
            Self::Decimal128 => Self::ProtoType::Decimal128,
            Self::Timestamp => Self::ProtoType::Timestamp,
            Self::Date32 => Self::ProtoType::Date32,
            Self::Date64 => Self::ProtoType::Date64,
            Self::Interval => Self::ProtoType::Interval,
            Self::Utf8 => Self::ProtoType::Utf8,
            Self::Binary => Self::ProtoType::Binary,
            Self::Struct => Self::ProtoType::Struct,
            Self::List => Self::ProtoType::List,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidDatatypeId => return Err(RayexecError::new("invalid")),
            Self::ProtoType::Any => Self::Any,
            Self::ProtoType::Null => Self::Null,
            Self::ProtoType::Boolean => Self::Boolean,
            Self::ProtoType::Int8 => Self::Int8,
            Self::ProtoType::Int16 => Self::Int16,
            Self::ProtoType::Int32 => Self::Int32,
            Self::ProtoType::Int64 => Self::Int64,
            Self::ProtoType::Int128 => Self::Int128,
            Self::ProtoType::Uint8 => Self::UInt8,
            Self::ProtoType::Uint16 => Self::UInt16,
            Self::ProtoType::Uint32 => Self::UInt32,
            Self::ProtoType::Uint64 => Self::UInt64,
            Self::ProtoType::Uint128 => Self::UInt128,
            Self::ProtoType::Float16 => Self::Float16,
            Self::ProtoType::Float32 => Self::Float32,
            Self::ProtoType::Float64 => Self::Float64,
            Self::ProtoType::Decimal64 => Self::Decimal64,
            Self::ProtoType::Decimal128 => Self::Decimal128,
            Self::ProtoType::Timestamp => Self::Timestamp,
            Self::ProtoType::Date32 => Self::Date32,
            Self::ProtoType::Date64 => Self::Date64,
            Self::ProtoType::Interval => Self::Interval,
            Self::ProtoType::Utf8 => Self::Utf8,
            Self::ProtoType::Binary => Self::Binary,
            Self::ProtoType::Struct => Self::Struct,
            Self::ProtoType::List => Self::List,
        })
    }
}

impl fmt::Display for DataTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => write!(f, "Any"),
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
    pub precision: u8,
    pub scale: i8,
}

impl DecimalTypeMeta {
    pub const fn new(precision: u8, scale: i8) -> Self {
        DecimalTypeMeta { precision, scale }
    }
}

impl ProtoConv for DecimalTypeMeta {
    type ProtoType = rayexec_proto::generated::schema::DecimalTypeMeta;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            precision: self.precision as i32,
            scale: self.scale as i32,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(DecimalTypeMeta {
            precision: proto.precision.try_into().context("invalid i8")?,
            scale: proto.scale.try_into().context("invalid i8")?,
        })
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

impl ProtoConv for TimestampTypeMeta {
    type ProtoType = rayexec_proto::generated::schema::TimestampTypeMeta;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            unit: self.unit.to_proto()? as i32,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            unit: TimeUnit::from_proto(proto.unit())?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl ProtoConv for TimeUnit {
    type ProtoType = rayexec_proto::generated::schema::TimeUnit;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::Second => Self::ProtoType::Second,
            Self::Millisecond => Self::ProtoType::Millisecond,
            Self::Microsecond => Self::ProtoType::Microsecond,
            Self::Nanosecond => Self::ProtoType::Nanosecond,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidTimeUnit => return Err(RayexecError::new("invalid")),
            Self::ProtoType::Second => Self::Second,
            Self::ProtoType::Millisecond => Self::Millisecond,
            Self::ProtoType::Microsecond => Self::Microsecond,
            Self::ProtoType::Nanosecond => Self::Nanosecond,
        })
    }
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
    pub fields: Vec<Field>,
}

impl ProtoConv for StructTypeMeta {
    type ProtoType = rayexec_proto::generated::schema::StructTypeMeta;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        let fields = self
            .fields
            .iter()
            .map(|f| f.to_proto())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::ProtoType { fields })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        let fields = proto
            .fields
            .into_iter()
            .map(Field::from_proto)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { fields })
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

impl ProtoConv for ListTypeMeta {
    type ProtoType = rayexec_proto::generated::schema::ListTypeMeta;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            datatype: Some(Box::new(self.datatype.to_proto()?)),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            datatype: Box::new(DataType::from_proto(*proto.datatype.required("datatype")?)?),
        })
    }
}

/// Supported data types.
///
/// This generally follows Arrow's type system, but is not restricted to it.
///
/// Some types may include additional metadata, which acts to refine the type
/// even further.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Constant null columns.
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
    Decimal64(DecimalTypeMeta),
    /// 128-bit decimal.
    Decimal128(DecimalTypeMeta),
    /// Timestamp
    Timestamp(TimestampTypeMeta),
    /// Days since epoch.
    Date32,
    /// Milliseconds since epoch.
    Date64,
    /// Some time interval with nanosecond resolution.
    Interval,
    Utf8,
    Binary,
    /// A struct of different types.
    Struct(StructTypeMeta),
    /// A list of values all of the same type.
    List(ListTypeMeta),
}

impl DataType {
    /// Try to create a default data type from the the data type id.
    ///
    /// Errors on attempts to create a data type from an id that we either don't
    /// have enough information about (struct, list) or can never be represented
    /// as a concrete data type (any).
    pub fn try_default_datatype(id: DataTypeId) -> Result<Self> {
        Ok(match id {
            DataTypeId::Any => {
                return Err(RayexecError::new("Cannot create a default Any datatype"))
            }
            DataTypeId::Null => DataType::Null,
            DataTypeId::Boolean => DataType::Boolean,
            DataTypeId::Int8 => DataType::Int8,
            DataTypeId::Int16 => DataType::Int16,
            DataTypeId::Int32 => DataType::Int32,
            DataTypeId::Int64 => DataType::Int64,
            DataTypeId::Int128 => DataType::Int128,
            DataTypeId::UInt8 => DataType::UInt8,
            DataTypeId::UInt16 => DataType::UInt16,
            DataTypeId::UInt32 => DataType::UInt32,
            DataTypeId::UInt64 => DataType::UInt64,
            DataTypeId::UInt128 => DataType::UInt128,
            DataTypeId::Float16 => DataType::Float16,
            DataTypeId::Float32 => DataType::Float32,
            DataTypeId::Float64 => DataType::Float64,
            DataTypeId::Decimal64 => DataType::Decimal64(DecimalTypeMeta::new(
                Decimal64Type::MAX_PRECISION,
                Decimal64Type::DEFAULT_SCALE,
            )),
            DataTypeId::Decimal128 => DataType::Decimal128(DecimalTypeMeta::new(
                Decimal128Type::MAX_PRECISION,
                Decimal128Type::DEFAULT_SCALE,
            )),
            DataTypeId::Timestamp => DataType::Timestamp(TimestampTypeMeta {
                unit: TimeUnit::Microsecond,
            }),
            DataTypeId::Date32 => DataType::Date32,
            DataTypeId::Date64 => DataType::Date64,
            DataTypeId::Interval => DataType::Interval,
            DataTypeId::Utf8 => DataType::Utf8,
            DataTypeId::Binary => DataType::Binary,
            DataTypeId::Struct => {
                return Err(RayexecError::new("Cannot create a default Struct datatype"))
            }
            DataTypeId::List => {
                return Err(RayexecError::new("Cannot create a default List datatype"))
            }
        })
    }

    /// Get the data type id from the data type.
    pub const fn datatype_id(&self) -> DataTypeId {
        match self {
            DataType::Null => DataTypeId::Null,
            DataType::Boolean => DataTypeId::Boolean,
            DataType::Int8 => DataTypeId::Int8,
            DataType::Int16 => DataTypeId::Int16,
            DataType::Int32 => DataTypeId::Int32,
            DataType::Int64 => DataTypeId::Int64,
            DataType::Int128 => DataTypeId::Int128,
            DataType::UInt8 => DataTypeId::UInt8,
            DataType::UInt16 => DataTypeId::UInt16,
            DataType::UInt32 => DataTypeId::UInt32,
            DataType::UInt64 => DataTypeId::UInt64,
            DataType::UInt128 => DataTypeId::UInt128,
            DataType::Float16 => DataTypeId::Float16,
            DataType::Float32 => DataTypeId::Float32,
            DataType::Float64 => DataTypeId::Float64,
            DataType::Decimal64(_) => DataTypeId::Decimal64,
            DataType::Decimal128(_) => DataTypeId::Decimal128,
            DataType::Timestamp(_) => DataTypeId::Timestamp,
            DataType::Date32 => DataTypeId::Date32,
            DataType::Date64 => DataTypeId::Date64,
            DataType::Interval => DataTypeId::Interval,
            DataType::Utf8 => DataTypeId::Utf8,
            DataType::Binary => DataTypeId::Binary,
            DataType::Struct(_) => DataTypeId::Struct,
            DataType::List(_) => DataTypeId::List,
        }
    }

    pub fn physical_type(&self) -> Result<PhysicalType> {
        Ok(match self {
            DataType::Null => PhysicalType::UntypedNull,
            DataType::Boolean => PhysicalType::Boolean,
            DataType::Int8 => PhysicalType::Int8,
            DataType::Int16 => PhysicalType::Int16,
            DataType::Int32 => PhysicalType::Int32,
            DataType::Int64 => PhysicalType::Int64,
            DataType::Int128 => PhysicalType::Int128,
            DataType::UInt8 => PhysicalType::UInt8,
            DataType::UInt16 => PhysicalType::UInt16,
            DataType::UInt32 => PhysicalType::UInt32,
            DataType::UInt64 => PhysicalType::UInt64,
            DataType::UInt128 => PhysicalType::UInt128,
            DataType::Float16 => PhysicalType::Float16,
            DataType::Float32 => PhysicalType::Float32,
            DataType::Float64 => PhysicalType::Float64,
            DataType::Decimal64(_) => PhysicalType::Int64,
            DataType::Decimal128(_) => PhysicalType::Int128,
            DataType::Timestamp(_) => PhysicalType::Int64,
            DataType::Date32 => PhysicalType::Int32,
            DataType::Date64 => PhysicalType::Int64,
            DataType::Interval => PhysicalType::Interval,
            DataType::Utf8 => PhysicalType::Utf8,
            DataType::Binary => PhysicalType::Binary,
            DataType::Struct(_) => not_implemented!("struct data type to physical type"),
            DataType::List(_) => PhysicalType::List,
        })
    }

    /// Return if this datatype is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    /// Return if this datatype is a list.
    pub const fn is_list(&self) -> bool {
        matches!(self, DataType::List(_))
    }

    pub const fn is_utf8(&self) -> bool {
        matches!(self, DataType::Utf8)
    }

    pub const fn is_primitive_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int128
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt128
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
        )
    }

    pub const fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int128
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt128
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal64(_)
                | DataType::Decimal128(_)
        )
    }

    pub const fn is_float(&self) -> bool {
        matches!(
            self,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
    }

    pub const fn is_decimal(&self) -> bool {
        matches!(self, DataType::Decimal64(_) | DataType::Decimal128(_))
    }

    pub fn try_get_decimal_type_meta(&self) -> Result<DecimalTypeMeta> {
        match self {
            Self::Decimal64(m) => Ok(*m),
            Self::Decimal128(m) => Ok(*m),
            other => Err(RayexecError::new(format!(
                "Cannot get decimal type meta from type {other}"
            ))),
        }
    }
}

impl ProtoConv for DataType {
    type ProtoType = rayexec_proto::generated::schema::DataType;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::schema::data_type::Value;
        use rayexec_proto::generated::schema::EmptyMeta;

        let value = match self {
            DataType::Null => Value::TypeNull(EmptyMeta {}),
            DataType::Boolean => Value::TypeBoolean(EmptyMeta {}),
            DataType::Int8 => Value::TypeInt8(EmptyMeta {}),
            DataType::Int16 => Value::TypeInt16(EmptyMeta {}),
            DataType::Int32 => Value::TypeInt32(EmptyMeta {}),
            DataType::Int64 => Value::TypeInt64(EmptyMeta {}),
            DataType::Int128 => Value::TypeInt128(EmptyMeta {}),
            DataType::UInt8 => Value::TypeUint8(EmptyMeta {}),
            DataType::UInt16 => Value::TypeUint16(EmptyMeta {}),
            DataType::UInt32 => Value::TypeUint32(EmptyMeta {}),
            DataType::UInt64 => Value::TypeUint64(EmptyMeta {}),
            DataType::UInt128 => Value::TypeUint128(EmptyMeta {}),
            DataType::Float16 => Value::TypeFloat16(EmptyMeta {}),
            DataType::Float32 => Value::TypeFloat32(EmptyMeta {}),
            DataType::Float64 => Value::TypeFloat64(EmptyMeta {}),
            DataType::Decimal64(m) => Value::TypeDecimal64(m.to_proto()?),
            DataType::Decimal128(m) => Value::TypeDecimal128(m.to_proto()?),
            DataType::Timestamp(m) => Value::TypeTimestamp(m.to_proto()?),
            DataType::Date32 => Value::TypeDate32(EmptyMeta {}),
            DataType::Date64 => Value::TypeDate64(EmptyMeta {}),
            DataType::Interval => Value::TypeInterval(EmptyMeta {}),
            DataType::Utf8 => Value::TypeUtf8(EmptyMeta {}),
            DataType::Binary => Value::TypeBinary(EmptyMeta {}),
            DataType::Struct(m) => Value::TypeStruct(m.to_proto()?),
            DataType::List(m) => Value::TypeList(Box::new(m.to_proto()?)),
        };
        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::schema::data_type::Value;

        Ok(match proto.value.required("value")? {
            Value::TypeNull(_) => DataType::Null,
            Value::TypeBoolean(_) => DataType::Boolean,
            Value::TypeInt8(_) => DataType::Int8,
            Value::TypeInt16(_) => DataType::Int16,
            Value::TypeInt32(_) => DataType::Int32,
            Value::TypeInt64(_) => DataType::Int64,
            Value::TypeInt128(_) => DataType::Int128,
            Value::TypeUint8(_) => DataType::UInt8,
            Value::TypeUint16(_) => DataType::UInt16,
            Value::TypeUint32(_) => DataType::UInt32,
            Value::TypeUint64(_) => DataType::UInt64,
            Value::TypeUint128(_) => DataType::UInt128,
            Value::TypeFloat16(_) => DataType::Float16,
            Value::TypeFloat32(_) => DataType::Float32,
            Value::TypeFloat64(_) => DataType::Float64,
            Value::TypeDecimal64(m) => DataType::Decimal64(DecimalTypeMeta::from_proto(m)?),
            Value::TypeDecimal128(m) => DataType::Decimal128(DecimalTypeMeta::from_proto(m)?),
            Value::TypeTimestamp(m) => DataType::Timestamp(TimestampTypeMeta::from_proto(m)?),
            Value::TypeDate32(_) => DataType::Date32,
            Value::TypeDate64(_) => DataType::Date64,
            Value::TypeInterval(_) => DataType::Interval,
            Value::TypeUtf8(_) => DataType::Utf8,
            Value::TypeBinary(_) => DataType::Binary,
            Value::TypeStruct(m) => DataType::Struct(StructTypeMeta::from_proto(m)?),
            Value::TypeList(m) => DataType::List(ListTypeMeta::from_proto(*m)?),
        })
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::Decimal64(meta) => write!(f, "Decimal64({},{})", meta.precision, meta.scale),
            Self::Decimal128(meta) => write!(f, "Decimal128({},{})", meta.precision, meta.scale),
            Self::Timestamp(meta) => write!(f, "Timestamp({})", meta.unit),
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Interval => write!(f, "Interval"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::Binary => write!(f, "Binary"),
            Self::Struct(meta) => {
                write!(
                    f,
                    "Struct {{{}}}",
                    meta.fields
                        .iter()
                        .map(|field| format!("{}: {}", field.name, field.datatype))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataType::List(meta) => write!(f, "List[{}]", meta.datatype),
        }
    }
}
