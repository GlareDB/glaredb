pub mod decimal;
pub mod interval;
pub mod timestamp;

use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;

use decimal::{Decimal128Scalar, Decimal64Scalar};
use half::f16;
use interval::Interval;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use timestamp::TimestampScalar;

use crate::arrays::array::Array;
use crate::arrays::compute::cast::format::{
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
use crate::arrays::datatype::{
    DataType,
    DecimalTypeMeta,
    ListTypeMeta,
    TimeUnit,
    TimestampTypeMeta,
};
use crate::buffer::buffer_manager::NopBufferManager;

/// A single scalar value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BorrowedScalarValue<'a> {
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
    Struct(Vec<BorrowedScalarValue<'a>>),
    List(Vec<BorrowedScalarValue<'a>>),
}

// TODO: TBD if we want this. We may need to implement PartialEq to exact
// equality semantics for floats.
impl Eq for BorrowedScalarValue<'_> {}

impl Hash for BorrowedScalarValue<'_> {
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

pub type ScalarValue = BorrowedScalarValue<'static>;

impl BorrowedScalarValue<'_> {
    pub fn datatype(&self) -> DataType {
        match self {
            BorrowedScalarValue::Null => DataType::Null,
            BorrowedScalarValue::Boolean(_) => DataType::Boolean,
            BorrowedScalarValue::Float16(_) => DataType::Float16,
            BorrowedScalarValue::Float32(_) => DataType::Float32,
            BorrowedScalarValue::Float64(_) => DataType::Float64,
            BorrowedScalarValue::Int8(_) => DataType::Int8,
            BorrowedScalarValue::Int16(_) => DataType::Int16,
            BorrowedScalarValue::Int32(_) => DataType::Int32,
            BorrowedScalarValue::Int64(_) => DataType::Int64,
            BorrowedScalarValue::Int128(_) => DataType::Int128,
            BorrowedScalarValue::UInt8(_) => DataType::UInt8,
            BorrowedScalarValue::UInt16(_) => DataType::UInt16,
            BorrowedScalarValue::UInt32(_) => DataType::UInt32,
            BorrowedScalarValue::UInt64(_) => DataType::UInt64,
            BorrowedScalarValue::UInt128(_) => DataType::UInt128,
            BorrowedScalarValue::Decimal64(v) => {
                DataType::Decimal64(DecimalTypeMeta::new(v.precision, v.scale))
            }
            BorrowedScalarValue::Decimal128(v) => {
                DataType::Decimal128(DecimalTypeMeta::new(v.precision, v.scale))
            }
            BorrowedScalarValue::Date32(_) => DataType::Date32,
            BorrowedScalarValue::Date64(_) => DataType::Date64,
            BorrowedScalarValue::Timestamp(v) => {
                DataType::Timestamp(TimestampTypeMeta::new(v.unit))
            }
            BorrowedScalarValue::Interval(_) => DataType::Interval,
            BorrowedScalarValue::Utf8(_) => DataType::Utf8,
            BorrowedScalarValue::Binary(_) => DataType::Binary,
            BorrowedScalarValue::Struct(_fields) => unimplemented!(), // TODO: Fill out the meta
            BorrowedScalarValue::List(list) => match list.first() {
                Some(first) => DataType::List(ListTypeMeta {
                    datatype: Box::new(first.datatype()),
                }),
                None => DataType::List(ListTypeMeta {
                    datatype: Box::new(DataType::Null),
                }),
            },
        }
    }

    pub fn into_owned(self) -> ScalarValue {
        match self {
            Self::Null => ScalarValue::Null,
            Self::Boolean(v) => ScalarValue::Boolean(v),
            Self::Float16(v) => ScalarValue::Float16(v),
            Self::Float32(v) => ScalarValue::Float32(v),
            Self::Float64(v) => ScalarValue::Float64(v),
            Self::Int8(v) => ScalarValue::Int8(v),
            Self::Int16(v) => ScalarValue::Int16(v),
            Self::Int32(v) => ScalarValue::Int32(v),
            Self::Int64(v) => ScalarValue::Int64(v),
            Self::Int128(v) => ScalarValue::Int128(v),
            Self::UInt8(v) => ScalarValue::UInt8(v),
            Self::UInt16(v) => ScalarValue::UInt16(v),
            Self::UInt32(v) => ScalarValue::UInt32(v),
            Self::UInt64(v) => ScalarValue::UInt64(v),
            Self::UInt128(v) => ScalarValue::UInt128(v),
            Self::Decimal64(v) => ScalarValue::Decimal64(v),
            Self::Decimal128(v) => ScalarValue::Decimal128(v),
            Self::Date32(v) => ScalarValue::Date32(v),
            Self::Date64(v) => ScalarValue::Date64(v),
            Self::Timestamp(v) => ScalarValue::Timestamp(v),
            Self::Interval(v) => ScalarValue::Interval(v),
            Self::Utf8(v) => ScalarValue::Utf8(v.into_owned().into()),
            Self::Binary(v) => ScalarValue::Binary(v.into_owned().into()),
            Self::Struct(v) => ScalarValue::Struct(v.into_iter().map(|v| v.into_owned()).collect()),
            Self::List(v) => ScalarValue::List(v.into_iter().map(|v| v.into_owned()).collect()),
        }
    }

    /// Create an array of size `n` using the scalar value.
    pub fn as_array(&self, n: usize) -> Result<Array> {
        Array::new_constant(&NopBufferManager, self, n)
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

impl fmt::Display for BorrowedScalarValue<'_> {
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

macro_rules! impl_primitive_from {
    ($prim:ty, $variant:ident) => {
        impl From<$prim> for ScalarValue {
            fn from(value: $prim) -> Self {
                ScalarValue::$variant(value)
            }
        }
    };
}

impl_primitive_from!(bool, Boolean);

impl_primitive_from!(i8, Int8);
impl_primitive_from!(i16, Int16);
impl_primitive_from!(i32, Int32);
impl_primitive_from!(i64, Int64);
impl_primitive_from!(i128, Int128);

impl_primitive_from!(u8, UInt8);
impl_primitive_from!(u16, UInt16);
impl_primitive_from!(u32, UInt32);
impl_primitive_from!(u64, UInt64);
impl_primitive_from!(u128, UInt128);

impl_primitive_from!(f16, Float16);
impl_primitive_from!(f32, Float32);
impl_primitive_from!(f64, Float64);

impl_primitive_from!(Interval, Interval);

impl<'a> From<&'a str> for BorrowedScalarValue<'a> {
    fn from(value: &'a str) -> Self {
        BorrowedScalarValue::Utf8(Cow::Borrowed(value))
    }
}

impl From<String> for BorrowedScalarValue<'static> {
    fn from(value: String) -> Self {
        BorrowedScalarValue::Utf8(Cow::Owned(value))
    }
}

impl<'a> From<&'a [u8]> for BorrowedScalarValue<'a> {
    fn from(value: &'a [u8]) -> Self {
        BorrowedScalarValue::Binary(Cow::Borrowed(value))
    }
}

impl<'a, T: Into<BorrowedScalarValue<'a>>> From<Option<T>> for BorrowedScalarValue<'a> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => BorrowedScalarValue::Null,
        }
    }
}

impl From<Decimal64Scalar> for ScalarValue {
    fn from(value: Decimal64Scalar) -> Self {
        ScalarValue::Decimal64(value)
    }
}

impl From<Decimal128Scalar> for ScalarValue {
    fn from(value: Decimal128Scalar) -> Self {
        ScalarValue::Decimal128(value)
    }
}

impl ProtoConv for ScalarValue {
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
                    .map(ScalarValue::from_proto)
                    .collect::<Result<Vec<_>>>()?;
                Self::Struct(values)
            }
            Value::ScalarList(v) => {
                let values = v
                    .values
                    .into_iter()
                    .map(ScalarValue::from_proto)
                    .collect::<Result<Vec<_>>>()?;
                Self::List(values)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::util::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn scalar_i32_as_array() {
        let s = BorrowedScalarValue::from(14);
        let arr = s.as_array(4).unwrap();

        let expected = Array::try_from_iter([14, 14, 14, 14]).unwrap();

        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn scalar_utf8_as_array() {
        let s = BorrowedScalarValue::from("dog");
        let arr = s.as_array(4).unwrap();

        let expected = Array::try_from_iter(["dog", "dog", "dog", "dog"]).unwrap();

        assert_arrays_eq(&expected, &arr);
    }
}
