pub mod decimal;
pub mod interval;
pub mod timestamp;

use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;

use decimal::{Decimal128Scalar, Decimal64Scalar};
use half::f16;
use interval::Interval;
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use timestamp::TimestampScalar;

use crate::array::{ArrayOld, ArrayData};
use crate::bitmap::Bitmap;
use crate::compute::cast::format::{
    BoolFormatter,
    Date32Formatter,
    Date64Formatter,
    Decimal128Formatter,
    Decimal64Formatter,
    Float16Formatter,
    Float32Formatter,
    Float64Formatter,
    Formatter,
    Int128Formatter,
    Int16Formatter,
    Int32Formatter,
    Int64Formatter,
    Int8Formatter,
    IntervalFormatter,
    TimestampMicrosecondsFormatter,
    TimestampMillisecondsFormatter,
    TimestampNanosecondsFormatter,
    TimestampSecondsFormatter,
    UInt128Formatter,
    UInt16Formatter,
    UInt32Formatter,
    UInt64Formatter,
    UInt8Formatter,
};
use crate::datatype::{DataType, DecimalTypeMeta, ListTypeMeta, TimeUnit, TimestampTypeMeta};
use crate::executor::scalar::concat;
use crate::selection::SelectionVector;
use crate::storage::{
    BooleanStorage,
    GermanVarlenStorage,
    ListItemMetadata,
    ListStorage,
    PrimitiveStorage,
};

/// A single scalar value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue<'a> {
    Null,
    Boolean(bool),
    Float16(f16),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Decimal64(Decimal64Scalar),
    Decimal128(Decimal128Scalar),
    Date32(i32),
    Date64(i64),
    Timestamp(TimestampScalar),
    Interval(Interval),
    Utf8(Cow<'a, str>),
    Binary(Cow<'a, [u8]>),
    Struct(Vec<ScalarValue<'a>>),
    List(Vec<ScalarValue<'a>>),
}

// TODO: TBD if we want this. We may need to implement PartialEq to exact
// equality semantics for floats.
impl Eq for ScalarValue<'_> {}

impl Hash for ScalarValue<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0_u8.hash(state),
            Self::Boolean(v) => v.hash(state),
            Self::Float16(v) => v.to_le_bytes().hash(state),
            Self::Float32(v) => v.to_le_bytes().hash(state),
            Self::Float64(v) => v.to_le_bytes().hash(state),
            Self::Int8(v) => v.hash(state),
            Self::Int16(v) => v.hash(state),
            Self::Int32(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::Int128(v) => v.hash(state),
            Self::UInt8(v) => v.hash(state),
            Self::UInt16(v) => v.hash(state),
            Self::UInt32(v) => v.hash(state),
            Self::UInt64(v) => v.hash(state),
            Self::UInt128(v) => v.hash(state),
            Self::Decimal64(v) => v.hash(state),
            Self::Decimal128(v) => v.hash(state),
            Self::Date32(v) => v.hash(state),
            Self::Date64(v) => v.hash(state),
            Self::Timestamp(v) => v.hash(state),
            Self::Interval(v) => v.hash(state),
            Self::Utf8(v) => v.hash(state),
            Self::Binary(v) => v.hash(state),
            Self::Struct(v) => v.hash(state),
            Self::List(v) => v.hash(state),
        }
    }
}

pub type OwnedScalarValue = ScalarValue<'static>;

impl ScalarValue<'_> {
    pub fn datatype(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Float16(_) => DataType::Float16,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Int128(_) => DataType::Int128,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::UInt128(_) => DataType::UInt128,
            ScalarValue::Decimal64(v) => {
                DataType::Decimal64(DecimalTypeMeta::new(v.precision, v.scale))
            }
            ScalarValue::Decimal128(v) => {
                DataType::Decimal128(DecimalTypeMeta::new(v.precision, v.scale))
            }
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Timestamp(v) => DataType::Timestamp(TimestampTypeMeta::new(v.unit)),
            ScalarValue::Interval(_) => DataType::Interval,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::Struct(_fields) => unimplemented!(), // TODO: Fill out the meta
            ScalarValue::List(list) => match list.first() {
                Some(first) => DataType::List(ListTypeMeta {
                    datatype: Box::new(first.datatype()),
                }),
                None => DataType::List(ListTypeMeta {
                    datatype: Box::new(DataType::Null),
                }),
            },
        }
    }

    pub fn into_owned(self) -> OwnedScalarValue {
        match self {
            Self::Null => OwnedScalarValue::Null,
            Self::Boolean(v) => OwnedScalarValue::Boolean(v),
            Self::Float16(v) => OwnedScalarValue::Float16(v),
            Self::Float32(v) => OwnedScalarValue::Float32(v),
            Self::Float64(v) => OwnedScalarValue::Float64(v),
            Self::Int8(v) => OwnedScalarValue::Int8(v),
            Self::Int16(v) => OwnedScalarValue::Int16(v),
            Self::Int32(v) => OwnedScalarValue::Int32(v),
            Self::Int64(v) => OwnedScalarValue::Int64(v),
            Self::Int128(v) => OwnedScalarValue::Int128(v),
            Self::UInt8(v) => OwnedScalarValue::UInt8(v),
            Self::UInt16(v) => OwnedScalarValue::UInt16(v),
            Self::UInt32(v) => OwnedScalarValue::UInt32(v),
            Self::UInt64(v) => OwnedScalarValue::UInt64(v),
            Self::UInt128(v) => OwnedScalarValue::UInt128(v),
            Self::Decimal64(v) => OwnedScalarValue::Decimal64(v),
            Self::Decimal128(v) => OwnedScalarValue::Decimal128(v),
            Self::Date32(v) => OwnedScalarValue::Date32(v),
            Self::Date64(v) => OwnedScalarValue::Date64(v),
            Self::Timestamp(v) => OwnedScalarValue::Timestamp(v),
            Self::Interval(v) => OwnedScalarValue::Interval(v),
            Self::Utf8(v) => OwnedScalarValue::Utf8(v.into_owned().into()),
            Self::Binary(v) => OwnedScalarValue::Binary(v.into_owned().into()),
            Self::Struct(v) => {
                OwnedScalarValue::Struct(v.into_iter().map(|v| v.into_owned()).collect())
            }
            Self::List(v) => {
                OwnedScalarValue::List(v.into_iter().map(|v| v.into_owned()).collect())
            }
        }
    }

    /// Create an array of size `n` using the scalar value.
    pub fn as_array(&self, n: usize) -> Result<ArrayOld> {
        let data: ArrayData = match self {
            Self::Null => return Ok(ArrayOld::new_untyped_null_array(n)),
            Self::Boolean(v) => BooleanStorage(Bitmap::new_with_val(*v, 1)).into(),
            Self::Float16(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Float32(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Float64(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Int8(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Int16(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Int32(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Int64(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Int128(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::UInt8(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::UInt16(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::UInt32(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::UInt64(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::UInt128(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Decimal64(v) => PrimitiveStorage::from(vec![v.value]).into(),
            Self::Decimal128(v) => PrimitiveStorage::from(vec![v.value]).into(),
            Self::Date32(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Date64(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Timestamp(v) => PrimitiveStorage::from(vec![v.value]).into(),
            Self::Interval(v) => PrimitiveStorage::from(vec![*v]).into(),
            Self::Utf8(v) => GermanVarlenStorage::with_value(v.as_ref()).into(),
            Self::Binary(v) => GermanVarlenStorage::with_value(v.as_ref()).into(),
            Self::List(v) => {
                if v.is_empty() {
                    let metadata = ListItemMetadata { offset: 0, len: 0 };

                    ListStorage {
                        metadata: vec![metadata].into(),
                        array: ArrayOld::new_untyped_null_array(0),
                    }
                    .into()
                } else {
                    let arrays = v
                        .iter()
                        .map(|v| v.as_array(1))
                        .collect::<Result<Vec<_>>>()?;
                    let refs: Vec<_> = arrays.iter().collect();
                    let array = concat(&refs)?;

                    let metadata = ListItemMetadata {
                        offset: 0,
                        len: array.logical_len() as i32,
                    };

                    ListStorage {
                        metadata: vec![metadata].into(),
                        array,
                    }
                    .into()
                }
            }
            other => not_implemented!("{other:?} to array"), // Struct, List
        };

        let mut array = ArrayOld::new_with_array_data(self.datatype(), data);
        array.selection = Some(SelectionVector::repeated(n, 0).into());

        Ok(array)
    }

    pub fn try_as_bool(&self) -> Result<bool> {
        match self {
            Self::Boolean(b) => Ok(*b),
            other => Err(RayexecError::new(format!("Not a bool: {other}"))),
        }
    }

    pub fn try_as_usize(&self) -> Result<usize> {
        match self {
            Self::Int8(i) => Ok((*i).try_into()?),
            Self::Int16(i) => Ok((*i).try_into()?),
            Self::Int32(i) => Ok((*i).try_into()?),
            Self::Int64(i) => Ok((*i).try_into()?),
            Self::UInt8(i) => Ok(*i as usize),
            Self::UInt16(i) => Ok(*i as usize),
            Self::UInt32(i) => Ok(*i as usize),
            Self::UInt64(i) => Ok(*i as usize),
            other => Err(RayexecError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_i64(&self) -> Result<i64> {
        match self {
            Self::Int8(i) => Ok(*i as i64),
            Self::Int16(i) => Ok(*i as i64),
            Self::Int32(i) => Ok(*i as i64),
            Self::Int64(i) => Ok(*i),
            Self::UInt8(i) => Ok(*i as i64),
            Self::UInt16(i) => Ok(*i as i64),
            Self::UInt32(i) => Ok(*i as i64),
            Self::UInt64(i) => {
                if *i < i64::MAX as u64 {
                    Ok(*i as i64)
                } else {
                    Err(RayexecError::new("u64 too large to fit into an i64"))
                }
            }
            other => Err(RayexecError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_i32(&self) -> Result<i32> {
        match self {
            Self::Int8(i) => Ok(*i as i32),
            Self::Int16(i) => Ok(*i as i32),
            Self::Int32(i) => Ok(*i),
            Self::Int64(i) => {
                if *i < i32::MAX as i64 {
                    Ok(*i as i32)
                } else {
                    Err(RayexecError::new("i64 too large to fit into an i32"))
                }
            }
            Self::UInt8(i) => Ok(*i as i32),
            Self::UInt16(i) => Ok(*i as i32),
            Self::UInt32(i) => {
                if *i < i32::MAX as u32 {
                    Ok(*i as i32)
                } else {
                    Err(RayexecError::new("u32 too large to fit into an i32"))
                }
            }
            Self::UInt64(i) => {
                if *i < i32::MAX as u64 {
                    Ok(*i as i32)
                } else {
                    Err(RayexecError::new("u64 too large to fit into an i32"))
                }
            }
            other => Err(RayexecError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_str(&self) -> Result<&str> {
        match self {
            Self::Utf8(v) => Ok(v.as_ref()),
            other => Err(RayexecError::new(format!("Not a string: {other}"))),
        }
    }

    pub fn try_into_string(self) -> Result<String> {
        match self {
            Self::Utf8(v) => Ok(v.to_string()),
            other => Err(RayexecError::new(format!("Not a string: {other}"))),
        }
    }
}

impl fmt::Display for ScalarValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(v) => BoolFormatter::default().write(v, f),
            Self::Float16(v) => Float16Formatter::default().write(v, f),
            Self::Float32(v) => Float32Formatter::default().write(v, f),
            Self::Float64(v) => Float64Formatter::default().write(v, f),
            Self::Int8(v) => Int8Formatter::default().write(v, f),
            Self::Int16(v) => Int16Formatter::default().write(v, f),
            Self::Int32(v) => Int32Formatter::default().write(v, f),
            Self::Int64(v) => Int64Formatter::default().write(v, f),
            Self::Int128(v) => Int128Formatter::default().write(v, f),
            Self::UInt8(v) => UInt8Formatter::default().write(v, f),
            Self::UInt16(v) => UInt16Formatter::default().write(v, f),
            Self::UInt32(v) => UInt32Formatter::default().write(v, f),
            Self::UInt64(v) => UInt64Formatter::default().write(v, f),
            Self::UInt128(v) => UInt128Formatter::default().write(v, f),
            Self::Decimal64(v) => Decimal64Formatter::new(v.precision, v.scale).write(&v.value, f),
            Self::Decimal128(v) => {
                Decimal128Formatter::new(v.precision, v.scale).write(&v.value, f)
            }
            Self::Date32(v) => Date32Formatter.write(v, f),
            Self::Date64(v) => Date64Formatter.write(v, f),
            Self::Timestamp(v) => match v.unit {
                TimeUnit::Second => TimestampSecondsFormatter::default().write(&v.value, f),
                TimeUnit::Millisecond => {
                    TimestampMillisecondsFormatter::default().write(&v.value, f)
                }
                TimeUnit::Microsecond => {
                    TimestampMicrosecondsFormatter::default().write(&v.value, f)
                }
                TimeUnit::Nanosecond => TimestampNanosecondsFormatter::default().write(&v.value, f),
            },
            Self::Interval(v) => IntervalFormatter.write(v, f),
            Self::Utf8(v) => write!(f, "{}", v),
            Self::Binary(v) => write!(f, "{:X?}", v),
            Self::Struct(fields) => write!(
                f,
                "{{{}}}",
                fields
                    .iter()
                    .map(|typ| format!("{typ}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::List(list) => write!(
                f,
                "[{}]",
                list.iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl From<bool> for ScalarValue<'_> {
    fn from(value: bool) -> Self {
        ScalarValue::Boolean(value)
    }
}

impl From<f16> for ScalarValue<'_> {
    fn from(value: f16) -> Self {
        ScalarValue::Float16(value)
    }
}

impl From<f32> for ScalarValue<'_> {
    fn from(value: f32) -> Self {
        ScalarValue::Float32(value)
    }
}

impl From<f64> for ScalarValue<'_> {
    fn from(value: f64) -> Self {
        ScalarValue::Float64(value)
    }
}

impl From<i8> for ScalarValue<'_> {
    fn from(value: i8) -> Self {
        ScalarValue::Int8(value)
    }
}

impl From<i16> for ScalarValue<'_> {
    fn from(value: i16) -> Self {
        ScalarValue::Int16(value)
    }
}

impl From<i32> for ScalarValue<'_> {
    fn from(value: i32) -> Self {
        ScalarValue::Int32(value)
    }
}

impl From<i64> for ScalarValue<'_> {
    fn from(value: i64) -> Self {
        ScalarValue::Int64(value)
    }
}

impl From<u8> for ScalarValue<'_> {
    fn from(value: u8) -> Self {
        ScalarValue::UInt8(value)
    }
}

impl From<u16> for ScalarValue<'_> {
    fn from(value: u16) -> Self {
        ScalarValue::UInt16(value)
    }
}

impl From<u32> for ScalarValue<'_> {
    fn from(value: u32) -> Self {
        ScalarValue::UInt32(value)
    }
}

impl From<u64> for ScalarValue<'_> {
    fn from(value: u64) -> Self {
        ScalarValue::UInt64(value)
    }
}

impl From<Interval> for ScalarValue<'_> {
    fn from(value: Interval) -> Self {
        ScalarValue::Interval(value)
    }
}

impl<'a> From<&'a str> for ScalarValue<'a> {
    fn from(value: &'a str) -> Self {
        ScalarValue::Utf8(Cow::Borrowed(value))
    }
}

impl From<String> for ScalarValue<'static> {
    fn from(value: String) -> Self {
        ScalarValue::Utf8(Cow::Owned(value))
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

impl ProtoConv for OwnedScalarValue {
    type ProtoType = rayexec_proto::generated::expr::OwnedScalarValue;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::expr::owned_scalar_value::Value;
        use rayexec_proto::generated::expr::{EmptyScalar, ListScalar, StructScalar};

        let value = match self {
            Self::Null => Value::ScalarNull(EmptyScalar {}),
            Self::Boolean(v) => Value::ScalarBoolean(*v),
            Self::Int8(v) => Value::ScalarInt8(*v as i32),
            Self::Int16(v) => Value::ScalarInt16(*v as i32),
            Self::Int32(v) => Value::ScalarInt32(*v),
            Self::Int64(v) => Value::ScalarInt64(*v),
            Self::Int128(v) => Value::ScalarInt128(v.to_le_bytes().to_vec()),
            Self::UInt8(v) => Value::ScalarUint8(*v as u32),
            Self::UInt16(v) => Value::ScalarUint16(*v as u32),
            Self::UInt32(v) => Value::ScalarUint32(*v),
            Self::UInt64(v) => Value::ScalarUint64(*v),
            Self::UInt128(v) => Value::ScalarUint128(v.to_le_bytes().to_vec()),
            Self::Float16(v) => Value::ScalarFloat16(v.to_f32()),
            Self::Float32(v) => Value::ScalarFloat32(*v),
            Self::Float64(v) => Value::ScalarFloat64(*v),
            Self::Decimal64(v) => Value::ScalarDecimal64(v.to_proto()?),
            Self::Decimal128(v) => Value::ScalarDecimal128(v.to_proto()?),
            Self::Timestamp(v) => Value::ScalarTimestamp(v.to_proto()?),
            Self::Date32(v) => Value::ScalarDate32(*v),
            Self::Date64(v) => Value::ScalarDate64(*v),
            Self::Interval(v) => Value::ScalarInterval(v.to_proto()?),
            Self::Utf8(v) => Value::ScalarUtf8(v.clone().into()),
            Self::Binary(v) => Value::ScalarBinary(v.clone().into()),
            Self::Struct(v) => {
                let values = v.iter().map(|v| v.to_proto()).collect::<Result<Vec<_>>>()?;
                Value::ScalarStruct(StructScalar { values })
            }
            Self::List(v) => {
                let values = v.iter().map(|v| v.to_proto()).collect::<Result<Vec<_>>>()?;
                Value::ScalarList(ListScalar { values })
            }
        };
        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::expr::owned_scalar_value::Value;

        Ok(match proto.value.required("owned scalar value enum")? {
            Value::ScalarNull(_) => Self::Null,
            Value::ScalarBoolean(v) => Self::Boolean(v),
            Value::ScalarInt8(v) => Self::Int8(v.try_into()?),
            Value::ScalarInt16(v) => Self::Int16(v.try_into()?),
            Value::ScalarInt32(v) => Self::Int32(v),
            Value::ScalarInt64(v) => Self::Int64(v),
            Value::ScalarInt128(v) => Self::Int128(i128::from_le_bytes(
                v.try_into()
                    .map_err(|_| RayexecError::new("byte buffer not 16 bytes"))?,
            )),
            Value::ScalarUint8(v) => Self::UInt8(v.try_into()?),
            Value::ScalarUint16(v) => Self::UInt16(v.try_into()?),
            Value::ScalarUint32(v) => Self::UInt32(v),
            Value::ScalarUint64(v) => Self::UInt64(v),
            Value::ScalarUint128(v) => Self::UInt128(u128::from_le_bytes(
                v.try_into()
                    .map_err(|_| RayexecError::new("byte buffer not 16 bytes"))?,
            )),
            Value::ScalarFloat16(v) => Self::Float16(f16::from_f32(v)),
            Value::ScalarFloat32(v) => Self::Float32(v),
            Value::ScalarFloat64(v) => Self::Float64(v),
            Value::ScalarDecimal64(v) => Self::Decimal64(Decimal64Scalar::from_proto(v)?),
            Value::ScalarDecimal128(v) => Self::Decimal128(Decimal128Scalar::from_proto(v)?),
            Value::ScalarTimestamp(v) => Self::Timestamp(TimestampScalar::from_proto(v)?),
            Value::ScalarDate32(v) => Self::Date32(v),
            Value::ScalarDate64(v) => Self::Date64(v),
            Value::ScalarInterval(v) => Self::Interval(Interval::from_proto(v)?),
            Value::ScalarUtf8(v) => Self::Utf8(v.into()),
            Value::ScalarBinary(v) => Self::Binary(v.into()),
            Value::ScalarStruct(v) => {
                let values = v
                    .values
                    .into_iter()
                    .map(OwnedScalarValue::from_proto)
                    .collect::<Result<Vec<_>>>()?;
                Self::Struct(values)
            }
            Value::ScalarList(v) => {
                let values = v
                    .values
                    .into_iter()
                    .map(OwnedScalarValue::from_proto)
                    .collect::<Result<Vec<_>>>()?;
                Self::List(values)
            }
        })
    }
}
