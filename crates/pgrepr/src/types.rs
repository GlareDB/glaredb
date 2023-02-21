use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use datafusion::arrow::array::Float16Array;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::{array::Array, datatypes::DataType as ArrowType};
use datafusion::scalar::ScalarValue;
use tokio_postgres::types::Type as PgType;

use crate::error::{PgReprError, Result};
use crate::format::Format;
use crate::writer::{BinaryWriter, TextWriter, Writer};

/// Returns a compatible postgres type for the arrow datatype.
///
/// If the type hint is not-none, it returns the type type inside the option.
pub fn arrow_to_pg_type(df_type: &ArrowType, type_hint: Option<PgType>) -> PgType {
    // TODO: Create pseudo types for unsigned integers and use them.
    type_hint.unwrap_or(match df_type {
        &ArrowType::Boolean => PgType::BOOL,
        &ArrowType::Int8 | &ArrowType::Int16 => PgType::INT2,
        &ArrowType::Int32 => PgType::INT4,
        &ArrowType::Int64 => PgType::INT8,
        &ArrowType::Float16 | &ArrowType::Float32 => PgType::FLOAT4,
        &ArrowType::Float64 => PgType::FLOAT8,
        &ArrowType::Utf8 => PgType::TEXT,
        &ArrowType::Binary => PgType::BYTEA,
        &ArrowType::Timestamp(_, None) => PgType::TIMESTAMP,
        &ArrowType::Timestamp(_, Some(_)) => PgType::TIMESTAMPTZ,
        &ArrowType::Time64(_) => PgType::TIME,
        &ArrowType::Date32 => PgType::DATE,
        // TODO: Intervals, numerics: They are a little complicated since not
        // directly supported by the tokio-postgres library, need to implement
        // them explicitly. We might be better having our own `ScalarValue`
        // enum so as to move away from Arrow restrictions. Shouldn't be much
        // work since all we use these values for is for encoding/decoding.

        // When there's a type we aren't really familiar with, we want to
        // return text in that case (literally!). We just want to send a
        // text representation of the datatype.
        _ => return PgType::TEXT,
    })
}

/// Encodes the array value as a postgres compatible value according to the
/// given format ("text" or "binary").
pub fn encode_array_value(
    buf: &mut BytesMut,
    format: Format,
    array: &Arc<dyn Array>,
    row_idx: usize,
    pg_type: &PgType,
) -> Result<()> {
    let scalar = try_scalar_from_array(array, row_idx)?;

    if scalar.is_null() {
        buf.put_i32(-1);
        return Ok(());
    }

    let (from, to) = (array.data_type(), pg_type);

    // Write a placeholder length.
    let len_idx = buf.len();
    buf.put_i32(0);

    match format {
        Format::Text => encode_array_not_null_value::<TextWriter>(buf, scalar, from, to),
        Format::Binary => encode_array_not_null_value::<BinaryWriter>(buf, scalar, from, to),
    }?;

    // Note the value of length does not include itself.
    let val_len = buf.len() - len_idx - 4;
    let val_len = i32::try_from(val_len).map_err(|_| PgReprError::MessageTooLarge(val_len))?;
    buf[len_idx..len_idx + 4].copy_from_slice(&i32::to_be_bytes(val_len));

    Ok(())
}

/// Per writer implementation for encoding non-null array values.
fn encode_array_not_null_value<W: Writer>(
    buf: &mut BytesMut,
    scalar: ScalarValue,
    from: &ArrowType,
    to: &PgType,
) -> Result<()> {
    match (from, to, scalar) {
        (&ArrowType::Boolean, &PgType::BOOL, ScalarValue::Boolean(Some(v))) => {
            W::write_bool(buf, v)
        }
        (&ArrowType::Int8, &PgType::INT2, ScalarValue::Int8(Some(v))) => {
            W::write_int2(buf, v as i16)
        }
        (&ArrowType::Int16, &PgType::INT2, ScalarValue::Int16(Some(v))) => W::write_int2(buf, v),
        (&ArrowType::Int32, &PgType::INT4, ScalarValue::Int32(Some(v))) => W::write_int4(buf, v),
        (&ArrowType::Int64, &PgType::INT8, ScalarValue::Int64(Some(v))) => W::write_int8(buf, v),
        (&ArrowType::Float16, &PgType::FLOAT4, ScalarValue::Float32(Some(v))) => {
            W::write_float4(buf, v)
        }
        (&ArrowType::Float32, &PgType::FLOAT4, ScalarValue::Float32(Some(v))) => {
            W::write_float4(buf, v)
        }
        (&ArrowType::Float64, &PgType::FLOAT8, ScalarValue::Float64(Some(v))) => {
            W::write_float8(buf, v)
        }
        (&ArrowType::Utf8, &PgType::TEXT, ScalarValue::Utf8(Some(v))) => W::write_text(buf, v),
        (&ArrowType::Binary, &PgType::BYTEA, ScalarValue::Binary(Some(v))) => {
            W::write_bytea(buf, v)
        }
        (
            &ArrowType::Timestamp(TimeUnit::Microsecond, None),
            &PgType::TIMESTAMP,
            ScalarValue::TimestampMicrosecond(Some(v), None),
        ) => {
            let seconds_since_epoch = v / 1_000_000;
            let micro_seconds = (v % 1_000_000).unsigned_abs() as u32;
            W::write_timestamp(
                buf,
                NaiveDateTime::from_timestamp_opt(seconds_since_epoch, micro_seconds * 1000)
                    .expect("scalar value should always return a valid timestamp"),
            )
        }
        (
            &ArrowType::Timestamp(TimeUnit::Microsecond, Some(_)),
            &PgType::TIMESTAMPTZ,
            ScalarValue::TimestampMicrosecond(Some(v), Some(_)),
        ) => {
            // Since timestamp is always relative to UTC epoch, we can directly
            // use that. Postgres never cares about the original timezone and
            // always returns the result in UTC.
            let seconds_since_epoch = v / 1_000_000;
            let micro_seconds = (v % 1_000_000).unsigned_abs() as u32;
            let utc_dt =
                NaiveDateTime::from_timestamp_opt(seconds_since_epoch, micro_seconds * 1000)
                    .expect("scalar value should always return a valid timestamp");
            W::write_timestamptz(buf, DateTime::<Utc>::from_utc(utc_dt, Utc))
        }
        (
            &ArrowType::Time64(TimeUnit::Microsecond),
            &PgType::TIME,
            ScalarValue::Time64Microsecond(Some(v)),
        ) => {
            let num_seconds_from_midnight = (v / 1_000_000) as u32;
            let micro_seconds = (v % 1_000_000) as u32;
            W::write_time(
                buf,
                NaiveTime::from_num_seconds_from_midnight_opt(
                    num_seconds_from_midnight,
                    micro_seconds * 1000,
                )
                .expect("scalar value should always return a valid time"),
            )
        }
        (&ArrowType::Date32, &PgType::DATE, ScalarValue::Date32(Some(v))) => {
            let epoch_date =
                NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date should be a valid date");
            let days_since_epoch = Days::new(v.unsigned_abs() as u64);
            let this_date = if v > 0 {
                epoch_date + days_since_epoch
            } else {
                epoch_date - days_since_epoch
            };
            W::write_date(buf, this_date)
        }
        (_, &PgType::TEXT, scalar) => {
            // Here we don't know how to process the arrow data-type. In these
            // cases it's recommended that we return the string representation
            // (along-with telling postgres that we're returning a TEXT).
            W::write_text(buf, format!("{scalar}"))
        }
        (from, to, _) => {
            // This should be unreachable. We definitely never want to convert
            // something that we can't. For the cases where we don't have an
            // appropriate conversion possible, the PG type should always be
            // TEXT in those cases.
            Err(PgReprError::InternalError(format!(
                "unable to convert ArrowType({}) to PostgresType({})",
                from, to,
            )))
        }
    }
}

/// Returns the most suitable scalar value for the array value.
fn try_scalar_from_array(array: &Arc<dyn Array>, row_idx: usize) -> Result<ScalarValue> {
    match ScalarValue::try_from_array(array, row_idx) {
        Ok(scalar) => Ok(scalar),
        Err(_) => {
            // This data-type is not supported by arrow. Try to find a suitable
            // conversion if possible, else error!
            match array.data_type() {
                &ArrowType::Float16 => {
                    // To ScalarValue::Float32
                    let array = array.as_any().downcast_ref::<Float16Array>().unwrap();
                    Ok(ScalarValue::Float32(match array.is_null(row_idx) {
                        true => None,
                        false => Some(array.value(row_idx).to_f32()),
                    }))
                }
                _ => Err(PgReprError::UnsupportedArrowType(
                    array.data_type().to_owned(),
                )),
            }
        }
    }
}
