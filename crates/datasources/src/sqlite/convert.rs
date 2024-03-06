use async_sqlite::rusqlite::types::{Value, ValueRef};
use async_sqlite::rusqlite::Rows;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::arrow::array::{
    ArrayBuilder,
    BinaryBuilder,
    BooleanBuilder,
    Date32Builder,
    Float64Builder,
    Int64Builder,
    StringBuilder,
    Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
    UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use repr::str::encode;

use super::errors::SqliteError;
use crate::sqlite::errors::Result;

#[derive(Debug, Clone)]
pub struct Converter {
    schema: SchemaRef,
}

impl Converter {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }

    pub fn create_record_batch(&self, rows: &mut Rows<'_>) -> Result<Option<RecordBatch>> {
        const RECORD_BATCH_CAPACITY: usize = 1000;

        let mut array_builders = self
            .schema
            .fields()
            .into_iter()
            .map(|field| {
                let builder: Box<dyn ArrayBuilder> = match field.data_type() {
                    DataType::Boolean => {
                        Box::new(BooleanBuilder::with_capacity(RECORD_BATCH_CAPACITY))
                    }
                    DataType::UInt64 => {
                        // sqlite can't produce this type, but for
                        // consistency with other glaredb count
                        // interfaces, we might
                        Box::new(UInt64Builder::with_capacity(RECORD_BATCH_CAPACITY))
                    }
                    DataType::Int64 => Box::new(Int64Builder::with_capacity(RECORD_BATCH_CAPACITY)),
                    DataType::Float64 => {
                        Box::new(Float64Builder::with_capacity(RECORD_BATCH_CAPACITY))
                    }
                    DataType::Utf8 => Box::new(StringBuilder::with_capacity(
                        RECORD_BATCH_CAPACITY,
                        10 * RECORD_BATCH_CAPACITY,
                    )),
                    DataType::Binary => Box::new(BinaryBuilder::with_capacity(
                        RECORD_BATCH_CAPACITY,
                        10 * RECORD_BATCH_CAPACITY,
                    )),
                    DataType::Date32 => {
                        Box::new(Date32Builder::with_capacity(RECORD_BATCH_CAPACITY))
                    }
                    DataType::Time64(TimeUnit::Microsecond) => Box::new(
                        Time64MicrosecondBuilder::with_capacity(RECORD_BATCH_CAPACITY),
                    ),
                    DataType::Timestamp(TimeUnit::Microsecond, None) => Box::new(
                        TimestampMicrosecondBuilder::with_capacity(RECORD_BATCH_CAPACITY),
                    ),
                    _ => unreachable!(),
                };
                builder
            })
            .collect::<Vec<_>>();

        let mut num_rows = 0;

        let mut str_buf = String::new();

        while let Some(row) = rows.next()? {
            num_rows += 1;

            for (col_idx, field) in self.schema.fields().into_iter().enumerate() {
                let val_ref = row
                    .get_ref(col_idx)
                    .map_err(|_| SqliteError::MissingDataForColumn(col_idx))?;

                match field.data_type() {
                    DataType::Boolean => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<BooleanBuilder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => builder.append_value(i != 0),
                            ValueRef::Real(r) => builder.append_value(r != 0_f64),
                            ValueRef::Text(t) => {
                                if t.eq_ignore_ascii_case(b"t")
                                    || t.eq_ignore_ascii_case(b"true")
                                    || t.eq_ignore_ascii_case(b"1")
                                {
                                    builder.append_value(true);
                                } else if t.eq_ignore_ascii_case(b"f")
                                    || t.eq_ignore_ascii_case(b"false")
                                    || t.eq_ignore_ascii_case(b"0")
                                {
                                    builder.append_value(false);
                                } else if t.is_empty() || t.eq_ignore_ascii_case(b"null") {
                                    builder.append_null();
                                } else {
                                    let t = std::str::from_utf8(t).unwrap();
                                    return Err(SqliteError::InvalidConversion {
                                        from: Value::Text(t.to_string()),
                                        to: DataType::Boolean,
                                    });
                                }
                            }
                            v => {
                                return Err(SqliteError::InvalidConversion {
                                    from: v.into(),
                                    to: DataType::Boolean,
                                });
                            }
                        };
                    }
                    DataType::Int64 => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => builder.append_value(i),
                            ValueRef::Real(r) => {
                                if r.trunc() == r {
                                    builder.append_value(r as i64);
                                } else {
                                    return Err(SqliteError::InvalidConversion {
                                        from: Value::Real(r),
                                        to: DataType::Int64,
                                    });
                                }
                            }
                            ValueRef::Text(t) => {
                                if t.is_empty() || t.eq_ignore_ascii_case(b"null") {
                                    builder.append_null();
                                } else {
                                    let t = std::str::from_utf8(t).unwrap();
                                    let i = t.parse::<i64>().map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Text(t.to_string()),
                                            to: DataType::Int64,
                                        }
                                    })?;
                                    builder.append_value(i);
                                }
                            }
                            v => {
                                return Err(SqliteError::InvalidConversion {
                                    from: v.into(),
                                    to: DataType::Int64,
                                });
                            }
                        };
                    }
                    DataType::Float64 => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => {
                                let f = i as f64;
                                let ii = f as i64;
                                if i == ii {
                                    builder.append_value(f);
                                } else {
                                    return Err(SqliteError::InvalidConversion {
                                        from: Value::Integer(i),
                                        to: DataType::Float64,
                                    });
                                }
                            }
                            ValueRef::Real(r) => builder.append_value(r),
                            ValueRef::Text(t) => {
                                if t.is_empty() || t.eq_ignore_ascii_case(b"null") {
                                    builder.append_null();
                                } else {
                                    let t = std::str::from_utf8(t).unwrap();
                                    let f = t.parse::<f64>().map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Text(t.to_string()),
                                            to: DataType::Float64,
                                        }
                                    })?;
                                    builder.append_value(f);
                                }
                            }
                            v => {
                                return Err(SqliteError::InvalidConversion {
                                    from: v.into(),
                                    to: DataType::Float64,
                                });
                            }
                        };
                    }
                    DataType::Utf8 => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => {
                                str_buf.clear();
                                encode::encode_int(&mut str_buf, i)?;
                                builder.append_value(&str_buf);
                            }
                            ValueRef::Real(r) => {
                                str_buf.clear();
                                encode::encode_float(&mut str_buf, r)?;
                                builder.append_value(&str_buf);
                            }
                            ValueRef::Text(t) => {
                                let t = std::str::from_utf8(t).unwrap();
                                builder.append_value(t);
                            }
                            ValueRef::Blob(b) => {
                                str_buf.clear();
                                encode::encode_binary(&mut str_buf, b)?;
                                builder.append_value(&str_buf);
                            }
                        };
                    }
                    DataType::Binary => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<BinaryBuilder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => {
                                builder.append_value(i.to_be_bytes().as_slice());
                            }
                            ValueRef::Real(r) => {
                                builder.append_value(r.to_be_bytes().as_slice());
                            }
                            ValueRef::Text(t) => {
                                if t.is_empty() || t.eq_ignore_ascii_case(b"null") {
                                    builder.append_null();
                                } else {
                                    builder.append_value(t);
                                }
                            }
                            ValueRef::Blob(b) => {
                                builder.append_value(b);
                            }
                        };
                    }
                    DataType::Date32 => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<Date32Builder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => {
                                let i = i32::try_from(i).map_err(|_| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Integer(i),
                                        to: DataType::Date32,
                                    }
                                })?;
                                builder.append_value(i);
                            }
                            ValueRef::Real(r) if r.fract() == 0_f64 => {
                                let i = r as i64;
                                let i = i32::try_from(i).map_err(|_| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Real(r),
                                        to: DataType::Date32,
                                    }
                                })?;
                                builder.append_value(i);
                            }
                            ValueRef::Text(t)
                                if t.is_empty() || t.eq_ignore_ascii_case(b"null") =>
                            {
                                builder.append_null();
                            }
                            ValueRef::Text(t) => {
                                // TODO: Support other str formats
                                let t = std::str::from_utf8(t).unwrap();
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let date =
                                    NaiveDate::parse_from_str(t, "%Y-%m-%d").map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Text(t.to_string()),
                                            to: DataType::Date32,
                                        }
                                    })?;
                                let num_days_since_epoch =
                                    date.signed_duration_since(epoch).num_days();
                                let i = i32::try_from(num_days_since_epoch).map_err(|_| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Text(t.to_string()),
                                        to: DataType::Date32,
                                    }
                                })?;
                                builder.append_value(i);
                            }
                            v => {
                                return Err(SqliteError::InvalidConversion {
                                    from: v.into(),
                                    to: DataType::Date32,
                                });
                            }
                        };
                    }
                    DataType::Time64(TimeUnit::Microsecond) => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<Time64MicrosecondBuilder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => {
                                let seconds_since_midnight = u32::try_from(i).map_err(|_| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Integer(i),
                                        to: DataType::Time64(TimeUnit::Microsecond),
                                    }
                                })?;
                                // Verify it's a valid time.
                                let _ = NaiveTime::from_num_seconds_from_midnight_opt(
                                    seconds_since_midnight,
                                    /* nsecs = */ 0,
                                )
                                .ok_or_else(|| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Integer(i),
                                        to: DataType::Time64(TimeUnit::Microsecond),
                                    }
                                })?;
                                let microseconds_since_midnight =
                                    seconds_since_midnight as i64 * 1_000_000;
                                builder.append_value(microseconds_since_midnight);
                            }
                            ValueRef::Real(r) => {
                                let seconds_since_midnight: u32 =
                                    (r as i64).try_into().map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Real(r),
                                            to: DataType::Time64(TimeUnit::Microsecond),
                                        }
                                    })?;
                                let sub_microseconds = {
                                    let fract = (r.fract() * 10_000_000_f64) as u32;
                                    if fract % 10_000_000 < 5 {
                                        fract / 10
                                    } else {
                                        (fract / 10) + 1
                                    }
                                };
                                // Verify it's a valid time
                                let _ = NaiveTime::from_num_seconds_from_midnight_opt(
                                    seconds_since_midnight,
                                    sub_microseconds * 1_000,
                                )
                                .ok_or_else(|| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Real(r),
                                        to: DataType::Time64(TimeUnit::Microsecond),
                                    }
                                })?;
                                let microseconds_since_midnight = (seconds_since_midnight as i64
                                    * 1_000_000)
                                    + sub_microseconds as i64;
                                builder.append_value(microseconds_since_midnight);
                            }
                            ValueRef::Text(t)
                                if t.is_empty() || t.eq_ignore_ascii_case(b"null") =>
                            {
                                builder.append_null();
                            }
                            ValueRef::Text(t) => {
                                // TODO: Support other str formats
                                let t = std::str::from_utf8(t).unwrap();
                                let epoch = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                                let time =
                                    NaiveTime::parse_from_str(t, "%H:%M:%S%.f").map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Text(t.to_string()),
                                            to: DataType::Time64(TimeUnit::Microsecond),
                                        }
                                    })?;
                                let duration_since_midnight = time.signed_duration_since(epoch);
                                let microseconds_since_midnight =
                                    duration_since_midnight.num_microseconds().unwrap();
                                builder.append_value(microseconds_since_midnight);
                            }
                            v => {
                                return Err(SqliteError::InvalidConversion {
                                    from: v.into(),
                                    to: DataType::Time64(TimeUnit::Microsecond),
                                });
                            }
                        };
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                        let builder = array_builders[col_idx]
                            .as_any_mut()
                            .downcast_mut::<TimestampMicrosecondBuilder>()
                            .unwrap();
                        match val_ref {
                            ValueRef::Null => builder.append_null(),
                            ValueRef::Integer(i) => {
                                let timestamp =
                                    NaiveDateTime::from_timestamp_opt(i, /* nsecs = */ 0)
                                        .ok_or_else(|| SqliteError::InvalidConversion {
                                            from: Value::Integer(i),
                                            to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                        })?;
                                let micros = timestamp.timestamp_micros();
                                builder.append_value(micros);
                            }
                            ValueRef::Real(r) => {
                                let seconds = r as i64;
                                let sub_nanos = (r.fract() * 1_000_000_000_f64) as u32;
                                let timestamp =
                                    NaiveDateTime::from_timestamp_opt(seconds, sub_nanos)
                                        .ok_or_else(|| SqliteError::InvalidConversion {
                                            from: Value::Real(r),
                                            to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                        })?;
                                let micros = timestamp.timestamp_micros();
                                builder.append_value(micros);
                            }
                            ValueRef::Text(t)
                                if t.is_empty() || t.eq_ignore_ascii_case(b"null") =>
                            {
                                builder.append_null();
                            }
                            ValueRef::Text(t) => {
                                // TODO: Support other str formats
                                let t = std::str::from_utf8(t).unwrap();
                                let timestamp =
                                    NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f")
                                        .map_err(|_| SqliteError::InvalidConversion {
                                            from: Value::Text(t.to_string()),
                                            to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                        })?;
                                let micros = timestamp.timestamp_micros();
                                builder.append_value(micros);
                            }
                            v => {
                                return Err(SqliteError::InvalidConversion {
                                    from: v.into(),
                                    to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                });
                            }
                        };
                    }
                    _ => unreachable!(),
                }
            }

            if num_rows >= RECORD_BATCH_CAPACITY {
                break;
            }
        }

        Ok(if num_rows == 0 {
            // The batch is empty
            None
        } else {
            let arrays = array_builders.into_iter().map(|mut b| b.finish()).collect();
            let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
            Some(batch)
        })
    }
}
