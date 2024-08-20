pub mod decimal;
pub mod interval;
pub mod timestamp;

use crate::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal64Array,
    Float32Array, Float64Array, Int128Array, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalArray, LargeBinaryArray, LargeUtf8Array, ListArray, NullArray, TimestampArray,
    UInt128Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
};
use crate::compute::cast::format::{
    BoolFormatter, Date32Formatter, Date64Formatter, Decimal128Formatter, Decimal64Formatter,
    Float32Formatter, Float64Formatter, Formatter, Int128Formatter, Int16Formatter, Int32Formatter,
    Int64Formatter, Int8Formatter, IntervalFormatter, TimestampMicrosecondsFormatter,
    TimestampMillisecondsFormatter, TimestampNanosecondsFormatter, TimestampSecondsFormatter,
    UInt128Formatter, UInt16Formatter, UInt32Formatter, UInt64Formatter, UInt8Formatter,
};
use crate::datatype::{DataType, DecimalTypeMeta, ListTypeMeta, TimeUnit, TimestampTypeMeta};
use decimal::{Decimal128Scalar, Decimal64Scalar};
use interval::Interval;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use timestamp::TimestampScalar;

/// A single scalar value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue<'a> {
    Null,
    Boolean(bool),
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
    LargeUtf8(Cow<'a, str>),
    Binary(Cow<'a, [u8]>),
    LargeBinary(Cow<'a, [u8]>),
    Struct(Vec<ScalarValue<'a>>),
    List(Vec<ScalarValue<'a>>),
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
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::Struct(_fields) => unimplemented!(), // TODO: Fill out the meta
            Self::List(list) => {
                let first = list.first().unwrap(); // TODO: Allow empty list scalars?
                DataType::List(ListTypeMeta {
                    datatype: Box::new(first.datatype()),
                })
            }
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
            Self::LargeUtf8(v) => OwnedScalarValue::LargeUtf8(v.into_owned().into()),
            Self::Binary(v) => OwnedScalarValue::Binary(v.into_owned().into()),
            Self::LargeBinary(v) => OwnedScalarValue::LargeBinary(v.into_owned().into()),
            Self::Struct(v) => {
                OwnedScalarValue::Struct(v.into_iter().map(|v| v.into_owned()).collect())
            }
            Self::List(v) => {
                OwnedScalarValue::List(v.into_iter().map(|v| v.into_owned()).collect())
            }
        }
    }

    /// Create an array of size `n` using the scalar value.
    pub fn as_array(&self, n: usize) -> Array {
        match self {
            Self::Null => Array::Null(NullArray::new(n)),
            Self::Boolean(v) => {
                Array::Boolean(BooleanArray::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Float32(v) => {
                Array::Float32(Float32Array::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Float64(v) => {
                Array::Float64(Float64Array::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Int8(v) => Array::Int8(Int8Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int16(v) => Array::Int16(Int16Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int32(v) => Array::Int32(Int32Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int64(v) => Array::Int64(Int64Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int128(v) => Array::Int128(Int128Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt8(v) => Array::UInt8(UInt8Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt16(v) => Array::UInt16(UInt16Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt32(v) => Array::UInt32(UInt32Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt64(v) => Array::UInt64(UInt64Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt128(v) => {
                Array::UInt128(UInt128Array::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Decimal64(v) => {
                let primitive = Int64Array::from_iter(std::iter::repeat(v.value).take(n));
                Array::Decimal64(Decimal64Array::new(v.precision, v.scale, primitive))
            }
            Self::Decimal128(v) => {
                let primitive = Int128Array::from_iter(std::iter::repeat(v.value).take(n));
                Array::Decimal128(Decimal128Array::new(v.precision, v.scale, primitive))
            }
            Self::Date32(v) => Array::Date32(Date32Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Date64(v) => Array::Date64(Date64Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Timestamp(v) => {
                let primitive = Int64Array::from_iter(std::iter::repeat(v.value).take(n));
                Array::Timestamp(TimestampArray::new(v.unit, primitive))
            }
            Self::Interval(v) => {
                Array::Interval(IntervalArray::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Utf8(v) => {
                Array::Utf8(Utf8Array::from_iter(std::iter::repeat(v.as_ref()).take(n)))
            }
            Self::LargeUtf8(v) => Array::LargeUtf8(LargeUtf8Array::from_iter(
                std::iter::repeat(v.as_ref()).take(n),
            )),
            Self::Binary(v) => Array::Binary(BinaryArray::from_iter(
                std::iter::repeat(v.as_ref()).take(n),
            )),
            Self::LargeBinary(v) => Array::LargeBinary(LargeBinaryArray::from_iter(
                std::iter::repeat(v.as_ref()).take(n),
            )),
            Self::Struct(_) => unimplemented!("struct into array"),
            Self::List(v) => {
                let children: Vec<_> = v.iter().map(|v| v.as_array(n)).collect();
                let refs: Vec<_> = children.iter().collect();
                let array = if refs.is_empty() {
                    ListArray::new_empty_with_n_rows(n)
                } else {
                    ListArray::try_from_children(&refs).expect("list array to build")
                };
                Array::List(array)
            }
        }
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
            Self::Utf8(v) | Self::LargeUtf8(v) => Ok(v.as_ref()),
            other => Err(RayexecError::new(format!("Not a string: {other}"))),
        }
    }

    pub fn try_into_string(self) -> Result<String> {
        match self {
            Self::Utf8(v) | Self::LargeUtf8(v) => Ok(v.to_string()),
            other => Err(RayexecError::new(format!("Not a string: {other}"))),
        }
    }
}

impl fmt::Display for ScalarValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(v) => BoolFormatter::default().write(v, f),
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
            Self::LargeUtf8(v) => write!(f, "{}", v),
            Self::Binary(v) => write!(f, "{:X?}", v),
            Self::LargeBinary(v) => write!(f, "{:X?}", v),
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

impl ProtoConv for OwnedScalarValue {
    type ProtoType = rayexec_proto::generated::expr::OwnedScalarValue;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::expr::{
            owned_scalar_value::Value, EmptyScalar, ListScalar, StructScalar,
        };

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
            Self::Float32(v) => Value::ScalarFloat32(*v),
            Self::Float64(v) => Value::ScalarFloat64(*v),
            Self::Decimal64(v) => Value::ScalarDecimal64(v.to_proto()?),
            Self::Decimal128(v) => Value::ScalarDecimal128(v.to_proto()?),
            Self::Timestamp(v) => Value::ScalarTimestamp(v.to_proto()?),
            Self::Date32(v) => Value::ScalarDate32(*v),
            Self::Date64(v) => Value::ScalarDate64(*v),
            Self::Interval(v) => Value::ScalarInterval(v.to_proto()?),
            Self::Utf8(v) => Value::ScalarUtf8(v.clone().into()),
            Self::LargeUtf8(v) => Value::ScalarLargeUtf8(v.clone().into()),
            Self::Binary(v) => Value::ScalarBinary(v.clone().into()),
            Self::LargeBinary(v) => Value::ScalarLargeBinary(v.clone().into()),
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
            Value::ScalarFloat32(v) => Self::Float32(v),
            Value::ScalarFloat64(v) => Self::Float64(v),
            Value::ScalarDecimal64(v) => Self::Decimal64(Decimal64Scalar::from_proto(v)?),
            Value::ScalarDecimal128(v) => Self::Decimal128(Decimal128Scalar::from_proto(v)?),
            Value::ScalarTimestamp(v) => Self::Timestamp(TimestampScalar::from_proto(v)?),
            Value::ScalarDate32(v) => Self::Date32(v),
            Value::ScalarDate64(v) => Self::Date64(v),
            Value::ScalarInterval(v) => Self::Interval(Interval::from_proto(v)?),
            Value::ScalarUtf8(v) => Self::Utf8(v.into()),
            Value::ScalarLargeUtf8(v) => Self::LargeUtf8(v.into()),
            Value::ScalarBinary(v) => Self::Binary(v.into()),
            Value::ScalarLargeBinary(v) => Self::LargeBinary(v.into()),
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