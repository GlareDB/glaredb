use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};
use chrono_tz::{Tz, TZ_VARIANTS};
use datafusion::arrow::array::{Array, Float16Array};
use datafusion::arrow::datatypes::{DataType as ArrowType, TimeUnit};
use datafusion::scalar::ScalarValue as DfScalar;
use decimal::Decimal128;
use once_cell::sync::Lazy;
use tokio_postgres::types::Type as PgType;

use crate::error::{PgReprError, Result};
use crate::format::Format;
use crate::reader::TextReader;
use crate::writer::{BinaryWriter, TextWriter};

static AVAILABLE_TIMEZONES: Lazy<HashMap<String, Tz>> = Lazy::new(|| {
    TZ_VARIANTS
        .iter()
        .map(|tz| (tz.name().to_owned(), *tz))
        .collect()
});

/// Scalasentation of Postgres value. This can be used as interface
/// between datafusion and postgres scalar values. All the scalar values
/// correspond to a postgres type.
///
/// An important thing to note is that a scalar value, even though corresponds
/// to a postgres type, it doesn't infer the type of in PG. We need extra
/// information to infer the type.
#[derive(Debug, PartialEq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Tz>),
    Time(NaiveTime),
    Date(NaiveDate),
    Decimal(Decimal128),
    // A datafusion value that isn't yet supported by us. Ultimately we want to
    // remove this and error in case we don't support something explicitly.
    Other(DfScalar),
}

impl Scalar {
    /// Returns the most suitable scalar value for the array value.
    pub fn try_from_array(
        array: &Arc<dyn Array>,
        row_idx: usize,
        as_type: &PgType, // TODO: Type hints
    ) -> Result<Scalar> {
        match DfScalar::try_from_array(array, row_idx) {
            Ok(scalar) => Ok(Self::from_datafusion(scalar, as_type)),
            Err(_) => {
                // This data-type is not supported by arrow. Try to find a suitable
                // conversion if possible, else error!
                match array.data_type() {
                    &ArrowType::Float16 => {
                        // To ScalarValue::Float32
                        let array = array.as_any().downcast_ref::<Float16Array>().unwrap();
                        Ok(match array.is_null(row_idx) {
                            true => Scalar::Null,
                            false => Scalar::Float4(array.value(row_idx).to_f32()),
                        })
                    }
                    _ => Err(PgReprError::UnsupportedArrowType(
                        array.data_type().to_owned(),
                    )),
                }
            }
        }
    }

    /// Returns true if the underlaying value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, &Self::Null)
    }

    pub fn encode_with_format(&self, format: Format, buf: &mut BytesMut) -> Result<()> {
        match format {
            Format::Text => self.encode::<TextWriter>(buf),
            Format::Binary => self.encode::<BinaryWriter>(buf),
        }
    }

    /// Encodes the scalar using the specified writer.
    pub fn encode<W>(&self, buf: &mut BytesMut) -> Result<()>
    where
        W: crate::writer::Writer,
    {
        match self {
            Self::Null => Ok(()),
            Self::Bool(v) => W::write_bool(buf, *v),
            Self::Int2(v) => W::write_int2(buf, *v),
            Self::Int4(v) => W::write_int4(buf, *v),
            Self::Int8(v) => W::write_int8(buf, *v),
            Self::Float4(v) => W::write_float4(buf, *v),
            Self::Float8(v) => W::write_float8(buf, *v),
            Self::Text(v) => W::write_text(buf, v),
            Self::Bytea(v) => W::write_bytea(buf, v),
            Self::Timestamp(v) => W::write_timestamp(buf, v),
            Self::TimestampTz(v) => W::write_timestamptz(buf, v),
            Self::Time(v) => W::write_time(buf, v),
            Self::Date(v) => W::write_date(buf, v),
            Self::Decimal(v) => W::write_decimal(buf, v),
            // If a type is not supported, we try to encode it as text.
            Self::Other(other) => W::write_any(buf, other),
        }
    }

    pub fn decode_with_format(format: Format, buf: &[u8], as_type: &PgType) -> Result<Self> {
        match format {
            Format::Text => Self::decode::<TextReader>(buf, as_type),
            Format::Binary => Err(PgReprError::UnsupportedPgTypeForDecode(as_type.to_owned())),
        }
    }

    pub fn decode<R>(buf: &[u8], as_type: &PgType) -> Result<Self>
    where
        R: crate::reader::Reader,
    {
        let scalar = match *as_type {
            PgType::BOOL => Self::Bool(R::read_bool(buf)?),
            PgType::INT2 => Self::Int2(R::read_int2(buf)?),
            PgType::INT4 => Self::Int4(R::read_int4(buf)?),
            PgType::INT8 => Self::Int8(R::read_int8(buf)?),
            PgType::FLOAT4 => Self::Float4(R::read_float4(buf)?),
            PgType::FLOAT8 => Self::Float8(R::read_float8(buf)?),
            PgType::TEXT => Self::Text(R::read_text(buf)?),
            _ => return Err(PgReprError::UnsupportedPgTypeForDecode(as_type.clone())),
        };
        Ok(scalar)
    }

    pub fn from_datafusion(
        value: DfScalar,
        _as_type: &PgType, // TODO: type hints
    ) -> Self {
        if value.is_null() {
            return Self::Null;
        }

        match value {
            DfScalar::Boolean(Some(v)) => Self::Bool(v),
            DfScalar::Int8(Some(v)) => Self::Int2(v as i16),
            DfScalar::Int16(Some(v)) => Self::Int2(v),
            DfScalar::Int32(Some(v)) => Self::Int4(v),
            DfScalar::Int64(Some(v)) => Self::Int8(v),
            DfScalar::Float32(Some(v)) => Self::Float4(v),
            DfScalar::Float64(Some(v)) => Self::Float8(v),
            DfScalar::Utf8(Some(v)) => Self::Text(v),
            DfScalar::Binary(Some(v)) => Self::Bytea(v),
            DfScalar::TimestampSecond(Some(v), None) => {
                Self::Timestamp(
                    DateTime::from_timestamp(v, /* nsecs = */ 0)
                        .unwrap()
                        .naive_utc(),
                )
            }
            DfScalar::TimestampMillisecond(Some(v), None) => {
                Self::Timestamp(DateTime::from_timestamp_millis(v).unwrap().naive_utc())
            }
            DfScalar::TimestampMicrosecond(Some(v), None) => {
                Self::Timestamp(DateTime::from_timestamp_micros(v).unwrap().naive_utc())
            }
            DfScalar::TimestampNanosecond(Some(v), None) => {
                Self::Timestamp(DateTime::from_timestamp_nanos(v).naive_utc())
            }
            DfScalar::TimestampSecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_opt(v, /* nsecs = */ 0).unwrap())
            }
            DfScalar::TimestampMillisecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_millis_opt(v).unwrap())
            }
            DfScalar::TimestampMicrosecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_micros(v).unwrap())
            }
            DfScalar::TimestampNanosecond(Some(v), Some(tz)) => {
                Self::TimestampTz(get_timezone(&tz).timestamp_nanos(v))
            }
            DfScalar::Time32Second(Some(v)) => Self::Time(
                DateTime::from_timestamp(v as i64, /* nsecs = */ 0)
                    .unwrap()
                    .time(),
            ),
            DfScalar::Time32Millisecond(Some(v)) => {
                Self::Time(DateTime::from_timestamp_millis(v as i64).unwrap().time())
            }
            DfScalar::Time64Microsecond(Some(v)) => {
                Self::Time(DateTime::from_timestamp_micros(v).unwrap().time())
            }
            DfScalar::Time64Nanosecond(Some(v)) => {
                Self::Time(DateTime::from_timestamp_nanos(v).time())
            }
            DfScalar::Date32(Some(v)) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let naive_date = epoch
                    .checked_add_signed(Duration::try_days(v as i64).unwrap())
                    .expect("scalar value should be a valid date");
                Self::Date(naive_date)
            }
            DfScalar::Decimal128(Some(v), _precision, scale) => {
                let decimal =
                    Decimal128::new(v, scale).expect("value should be a valid decimal128");
                Self::Decimal(decimal)
            }

            other => {
                debug_assert!(!other.is_null());
                Scalar::Other(other)
            }
        }
    }

    pub fn into_datafusion(self, as_type: &ArrowType) -> Result<DfScalar> {
        let scalar = match (self, as_type) {
            (Self::Null, ty) => ty
                .try_into()
                .map_err(|_| PgReprError::UnsupportedArrowType(ty.clone()))?,
            (Self::Bool(v), ArrowType::Boolean) => DfScalar::Boolean(Some(v)),
            (Self::Int2(v), ArrowType::Int8) => DfScalar::Int8(Some(v as i8)),
            (Self::Int2(v), ArrowType::Int16) => DfScalar::Int16(Some(v)),
            (Self::Int4(v), ArrowType::Int32) => DfScalar::Int32(Some(v)),
            (Self::Int8(v), ArrowType::Int64) => DfScalar::Int64(Some(v)),
            // TODO: f16
            (Self::Float4(v), ArrowType::Float32) => DfScalar::Float32(Some(v)),
            (Self::Float8(v), ArrowType::Float64) => DfScalar::Float64(Some(v)),
            (Self::Text(v), ArrowType::Utf8) => DfScalar::Utf8(Some(v)),
            (Self::Bytea(v), ArrowType::Binary) => DfScalar::Binary(Some(v)),
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Second, None)) => {
                DfScalar::TimestampSecond(Some(v.second() as i64), None)
            }
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Millisecond, None)) => {
                DfScalar::TimestampMillisecond(Some(v.and_utc().timestamp_millis()), None)
            }
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Microsecond, None)) => {
                DfScalar::TimestampMicrosecond(Some(v.and_utc().timestamp_micros()), None)
            }
            (Self::Timestamp(v), ArrowType::Timestamp(TimeUnit::Nanosecond, None)) => {
                DfScalar::TimestampNanosecond(v.and_utc().timestamp_nanos_opt(), None)
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Second, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::TimestampSecond(Some(v.timestamp()), Some(tz.clone()))
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Millisecond, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::TimestampMillisecond(Some(v.timestamp_millis()), Some(tz.clone()))
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Microsecond, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::TimestampMicrosecond(Some(v.timestamp_micros()), Some(tz.clone()))
            }
            (
                Self::TimestampTz(v),
                arrow_type @ ArrowType::Timestamp(TimeUnit::Nanosecond, Some(tz)),
            ) => {
                if tz.as_ref() != v.timezone().name() {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                let nanos = v.timestamp_nanos_opt().unwrap();
                DfScalar::TimestampNanosecond(Some(nanos), Some(tz.clone()))
            }
            (Self::Time(v), ArrowType::Time32(TimeUnit::Second)) => {
                DfScalar::Time32Second(Some(v.num_seconds_from_midnight() as i32))
            }
            (Self::Time(v), ArrowType::Time32(TimeUnit::Millisecond)) => {
                let secs = v.num_seconds_from_midnight() as i32;
                let sub_millis = (v.nanosecond() / 1_000_000) as i32;
                let millis = (secs * 1_000) + sub_millis;
                DfScalar::Time32Millisecond(Some(millis))
            }
            (Self::Time(v), ArrowType::Time64(TimeUnit::Microsecond)) => {
                let secs = v.num_seconds_from_midnight() as i64;
                let sub_micros = (v.nanosecond() / 1_000) as i64;
                let micros = (secs * 1_000_000) + sub_micros;
                DfScalar::Time64Microsecond(Some(micros))
            }
            (Self::Time(v), ArrowType::Time64(TimeUnit::Nanosecond)) => {
                let secs = v.num_seconds_from_midnight() as i64;
                let sub_nanos = (v.nanosecond()) as i64;
                let nanos = (secs * 1_000_000_000) + sub_nanos;
                DfScalar::Time64Nanosecond(Some(nanos))
            }
            (Self::Date(v), ArrowType::Date32) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days_since_epoch = v.signed_duration_since(epoch).num_days();
                DfScalar::Date32(Some(days_since_epoch as i32))
            }
            (Self::Decimal(v), arrow_type @ ArrowType::Decimal128(precision, scale)) => {
                if v.scale() != *scale {
                    return Err(PgReprError::InternalError(format!(
                        "cannot convert from {:?} to arrow type {:?}",
                        v, arrow_type
                    )));
                }
                DfScalar::Decimal128(Some(v.mantissa()), *precision, *scale)
            }
            (scalar, arrow_type) => {
                return Err(PgReprError::InternalError(format!(
                    "cannot convert from scalar {:?} to arrow type {:?}",
                    scalar, arrow_type
                )))
            }
        };
        Ok(scalar)
    }
}

// TODO: Figure out if this should be parsing time zone names like
// 'Australia/Melbourne' or offsets like '+03:00'.
fn get_timezone(tz: &str) -> Tz {
    *AVAILABLE_TIMEZONES.get(tz).unwrap_or(&chrono_tz::UTC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_timezone() {
        let tz = get_timezone("+00:00");
        assert_eq!(chrono_tz::UTC, tz);
    }
}
