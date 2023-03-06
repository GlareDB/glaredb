use std::{fmt::Write, sync::Arc};

use chrono::{Duration, TimeZone, Utc};
use datafusion::{
    arrow::{
        array::{Array, ArrayRef},
        compute::{cast_with_options, CastOptions},
        datatypes::{DataType, Field, Schema, TimeUnit},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    scalar::ScalarValue,
};
use repr::str::encode::*;

use crate::errors::{Error, Result};

fn is_literal_quotable(lit: &ScalarValue) -> bool {
    let not_quotable = matches!(
        lit,
        ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
    );
    !not_quotable
}

pub fn encode_literal_to_text(buf: &mut String, lit: &ScalarValue) -> Result<()> {
    if is_literal_quotable(lit) {
        buf.write_str("'")?;
    }
    match lit {
        ScalarValue::Int8(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Int16(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Int32(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Int64(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Float32(Some(v)) => encode_float(buf, *v)?,
        ScalarValue::Float64(Some(v)) => encode_float(buf, *v)?,
        ScalarValue::Utf8(Some(v)) => encode_string(buf, v)?,
        ScalarValue::Binary(Some(v)) => encode_binary(buf, v)?,
        ScalarValue::TimestampNanosecond(Some(v), tz) => {
            let naive = Utc.timestamp_nanos(*v).naive_utc();
            encode_utc_timestamp(buf, &naive, tz.is_some())?;
        }
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            let naive = Utc.timestamp_nanos(*v * 1_000).naive_utc();
            encode_utc_timestamp(buf, &naive, tz.is_some())?;
        }
        ScalarValue::Time64Nanosecond(Some(v)) => {
            let naive = Utc.timestamp_nanos(*v).naive_utc().time();
            encode_time(buf, &naive, /* tz = */ false)?;
        }
        ScalarValue::Time64Microsecond(Some(v)) => {
            let naive = Utc.timestamp_nanos(*v * 1_000).naive_utc().time();
            encode_time(buf, &naive, /* tz = */ false)?;
        }
        ScalarValue::Date32(Some(v)) => {
            let epoch = Utc.timestamp_nanos(0).naive_utc().date();
            let naive = epoch
                .checked_add_signed(Duration::days(*v as i64))
                .expect("scalar value should be a valid date");
            encode_date(buf, &naive)?;
        }
        s => return Err(Error::UnsupportedDatafusionScalar(s.get_datatype())),
    };
    if is_literal_quotable(lit) {
        buf.write_str("'")?;
    }
    Ok(())
}

const DEFAULT_CAST_OPTIONS: CastOptions = CastOptions {
    // If a cast fails we should rather report the error and fix it instead
    // of returning NULLs.
    safe: false,
};

fn normalize_column(column: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let dt = match column.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
        }
        DataType::Time64(TimeUnit::Microsecond) => DataType::Time64(TimeUnit::Nanosecond),
        _ => return Ok(Arc::clone(column)), // No need of any conversion
    };

    let array = cast_with_options(column, &dt, &DEFAULT_CAST_OPTIONS)?;
    Ok(array)
}

pub fn normalize_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
        let col = normalize_column(col)?;
        let field = Field::new(field.name(), col.data_type().clone(), field.is_nullable());
        columns.push(col);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}
