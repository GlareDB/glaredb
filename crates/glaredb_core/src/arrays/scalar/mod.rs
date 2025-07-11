pub mod decimal;
pub mod interval;
pub mod timestamp;
pub mod unwrap;

use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;

use decimal::{Decimal64Scalar, Decimal128Scalar};
use glaredb_error::{DbError, Result};
use half::f16;
use interval::Interval;
use serde::{Deserialize, Serialize};
use timestamp::TimestampScalar;

use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta};
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::functions::cast::format::{
    BoolFormatter,
    Date32Formatter,
    Date64Formatter,
    Decimal64Formatter,
    Decimal128Formatter,
    Float16Formatter,
    Float32Formatter,
    Float64Formatter,
    Formatter,
    Int8Formatter,
    Int16Formatter,
    Int32Formatter,
    Int64Formatter,
    Int128Formatter,
    IntervalFormatter,
    TimestampMicrosecondsFormatter,
    TimestampMillisecondsFormatter,
    TimestampNanosecondsFormatter,
    TimestampSecondsFormatter,
    UInt8Formatter,
    UInt16Formatter,
    UInt32Formatter,
    UInt64Formatter,
    UInt128Formatter,
};

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
            BorrowedScalarValue::Null => DataType::null(),
            BorrowedScalarValue::Boolean(_) => DataType::boolean(),
            BorrowedScalarValue::Float16(_) => DataType::float16(),
            BorrowedScalarValue::Float32(_) => DataType::float32(),
            BorrowedScalarValue::Float64(_) => DataType::float64(),
            BorrowedScalarValue::Int8(_) => DataType::int8(),
            BorrowedScalarValue::Int16(_) => DataType::int16(),
            BorrowedScalarValue::Int32(_) => DataType::int32(),
            BorrowedScalarValue::Int64(_) => DataType::int64(),
            BorrowedScalarValue::Int128(_) => DataType::int128(),
            BorrowedScalarValue::UInt8(_) => DataType::uint8(),
            BorrowedScalarValue::UInt16(_) => DataType::uint16(),
            BorrowedScalarValue::UInt32(_) => DataType::uint32(),
            BorrowedScalarValue::UInt64(_) => DataType::uint64(),
            BorrowedScalarValue::UInt128(_) => DataType::uint128(),
            BorrowedScalarValue::Decimal64(v) => {
                DataType::decimal64(DecimalTypeMeta::new(v.precision, v.scale))
            }
            BorrowedScalarValue::Decimal128(v) => {
                DataType::decimal128(DecimalTypeMeta::new(v.precision, v.scale))
            }
            BorrowedScalarValue::Date32(_) => DataType::date32(),
            BorrowedScalarValue::Date64(_) => DataType::date64(),
            BorrowedScalarValue::Timestamp(v) => {
                DataType::timestamp(TimestampTypeMeta::new(v.unit))
            }
            BorrowedScalarValue::Interval(_) => DataType::interval(),
            BorrowedScalarValue::Utf8(_) => DataType::utf8(),
            BorrowedScalarValue::Binary(_) => DataType::binary(),
            BorrowedScalarValue::Struct(_fields) => unimplemented!(), // TODO: Fill out the meta
            BorrowedScalarValue::List(list) => match list.first() {
                Some(first) => DataType::list(first.datatype()),
                None => DataType::list(DataType::null()),
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
        Array::new_constant(&DefaultBufferManager, self, n)
    }

    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn try_as_bool(&self) -> Result<bool> {
        match self {
            Self::Boolean(b) => Ok(*b),
            other => Err(DbError::new(format!("Not a bool: {other}"))),
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
            other => Err(DbError::new(format!("Not an integer: {other}"))),
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
                    Err(DbError::new("u64 too large to fit into an i64"))
                }
            }
            other => Err(DbError::new(format!("Not an integer: {other}"))),
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
                    Err(DbError::new("i64 too large to fit into an i32"))
                }
            }
            Self::UInt8(i) => Ok(*i as i32),
            Self::UInt16(i) => Ok(*i as i32),
            Self::UInt32(i) => {
                if *i < i32::MAX as u32 {
                    Ok(*i as i32)
                } else {
                    Err(DbError::new("u32 too large to fit into an i32"))
                }
            }
            Self::UInt64(i) => {
                if *i < i32::MAX as u64 {
                    Ok(*i as i32)
                } else {
                    Err(DbError::new("u64 too large to fit into an i32"))
                }
            }
            other => Err(DbError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_f64(&self) -> Result<f64> {
        match self {
            Self::Float16(v) => Ok(f64::from(*v)),
            Self::Float32(v) => Ok(*v as f64),
            Self::Float64(v) => Ok(*v),
            other => {
                let v_i32 = other.try_as_i32()?;
                Ok(v_i32 as f64)
            }
        }
    }

    pub fn try_as_str(&self) -> Result<&str> {
        match self {
            Self::Utf8(v) => Ok(v.as_ref()),
            other => Err(DbError::new(format!("Not a string: {other}"))),
        }
    }

    pub fn try_into_string(self) -> Result<String> {
        match self {
            Self::Utf8(v) => Ok(v.to_string()),
            other => Err(DbError::new(format!("Not a string: {other}"))),
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
            Self::Utf8(v) => write!(f, "{v}"),
            Self::Binary(v) => write!(f, "{v:X?}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

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
