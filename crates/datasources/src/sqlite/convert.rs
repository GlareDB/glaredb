use std::sync::Arc;

use async_sqlite::rusqlite::types::Value;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::arrow::array::{
    Array,
    BinaryBuilder,
    BooleanArray,
    Date32Array,
    Float64Array,
    Int64Array,
    StringBuilder,
    Time64MicrosecondArray,
    TimestampMicrosecondArray,
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

    pub fn create_record_batch(&self, data: Vec<Vec<Value>>) -> Result<RecordBatch> {
        let mut cols = Vec::new();

        for (col_idx, field) in self.schema.fields().into_iter().enumerate() {
            let col: Arc<dyn Array> = match field.data_type() {
                DataType::Boolean => Arc::new(
                    data.iter()
                        .map(|row| {
                            match row
                                .get(col_idx)
                                .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                            {
                                Value::Null => Ok(None),
                                Value::Integer(i) => Ok(Some(*i != 0)),
                                Value::Real(r) => Ok(Some(*r != 0_f64)),
                                Value::Text(t) => {
                                    if t.eq_ignore_ascii_case("t")
                                        || t.eq_ignore_ascii_case("true")
                                        || t.eq_ignore_ascii_case("1")
                                    {
                                        Ok(Some(true))
                                    } else if t.eq_ignore_ascii_case("f")
                                        || t.eq_ignore_ascii_case("false")
                                        || t.eq_ignore_ascii_case("0")
                                    {
                                        Ok(Some(false))
                                    } else if t.is_empty() || t.eq_ignore_ascii_case("null") {
                                        Ok(None)
                                    } else {
                                        Err(SqliteError::InvalidConversion {
                                            from: Value::Text(t.clone()),
                                            to: DataType::Boolean,
                                        })
                                    }
                                }
                                v => Err(SqliteError::InvalidConversion {
                                    from: v.clone(),
                                    to: DataType::Boolean,
                                }),
                            }
                        })
                        .collect::<Result<BooleanArray>>()?,
                ),
                DataType::Int64 => Arc::new(
                    data.iter()
                        .map(|row| {
                            match row
                                .get(col_idx)
                                .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                            {
                                Value::Null => Ok(None),
                                Value::Integer(i) => Ok(Some(*i)),
                                Value::Real(r) => {
                                    if r.trunc() == *r {
                                        Ok(Some(*r as i64))
                                    } else {
                                        Err(SqliteError::InvalidConversion {
                                            from: Value::Real(*r),
                                            to: DataType::Int64,
                                        })
                                    }
                                }
                                Value::Text(t) => {
                                    if t.is_empty() || t.eq_ignore_ascii_case("null") {
                                        Ok(None)
                                    } else {
                                        t.parse::<i64>().map(Some).map_err(|_| {
                                            SqliteError::InvalidConversion {
                                                from: Value::Text(t.clone()),
                                                to: DataType::Int64,
                                            }
                                        })
                                    }
                                }
                                v => Err(SqliteError::InvalidConversion {
                                    from: v.clone(),
                                    to: DataType::Int64,
                                }),
                            }
                        })
                        .collect::<Result<Int64Array>>()?,
                ),
                DataType::Float64 => Arc::new(
                    data.iter()
                        .map(|row| {
                            match row
                                .get(col_idx)
                                .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                            {
                                Value::Null => Ok(None),
                                Value::Integer(i) => {
                                    let f = *i as f64;
                                    let ii = f as i64;
                                    if *i == ii {
                                        Ok(Some(f))
                                    } else {
                                        Err(SqliteError::InvalidConversion {
                                            from: Value::Integer(*i),
                                            to: DataType::Float64,
                                        })
                                    }
                                }
                                Value::Real(r) => Ok(Some(*r)),
                                Value::Text(t) => {
                                    if t.is_empty() || t.eq_ignore_ascii_case("null") {
                                        Ok(None)
                                    } else {
                                        t.parse::<f64>().map(Some).map_err(|_| {
                                            SqliteError::InvalidConversion {
                                                from: Value::Text(t.clone()),
                                                to: DataType::Float64,
                                            }
                                        })
                                    }
                                }
                                v => Err(SqliteError::InvalidConversion {
                                    from: v.clone(),
                                    to: DataType::Float64,
                                }),
                            }
                        })
                        .collect::<Result<Float64Array>>()?,
                ),
                DataType::Utf8 => {
                    // Assuming an average length of each string to be 10
                    let mut builder = StringBuilder::with_capacity(data.len(), 10 * data.len());
                    let mut buf = String::new();
                    for row in data.iter() {
                        match row
                            .get(col_idx)
                            .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                        {
                            Value::Null => builder.append_null(),
                            Value::Integer(i) => {
                                buf.clear();
                                encode::encode_int(&mut buf, *i)?;
                                builder.append_value(&buf);
                            }
                            Value::Real(r) => {
                                buf.clear();
                                encode::encode_float(&mut buf, *r)?;
                                builder.append_value(&buf);
                            }
                            Value::Text(t) => builder.append_value(t),
                            Value::Blob(b) => {
                                buf.clear();
                                encode::encode_binary(&mut buf, b)?;
                                builder.append_value(&buf);
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Binary => {
                    // Assuming an average length of each vector to be 10
                    let mut builder = BinaryBuilder::with_capacity(data.len(), 10 * data.len());
                    for row in data.iter() {
                        match row
                            .get(col_idx)
                            .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                        {
                            Value::Null => builder.append_null(),
                            Value::Integer(i) => {
                                builder.append_value(i.to_be_bytes().as_slice());
                            }
                            Value::Real(r) => {
                                builder.append_value(r.to_be_bytes().as_slice());
                            }
                            Value::Text(t) => {
                                if t.is_empty() || t.eq_ignore_ascii_case("null") {
                                    builder.append_null();
                                } else {
                                    builder.append_value(t.as_bytes());
                                }
                            }
                            Value::Blob(b) => {
                                builder.append_value(b);
                            }
                        };
                    }
                    Arc::new(builder.finish())
                }
                DataType::Date32 => Arc::new(
                    data.iter()
                        .map(|row| {
                            match row
                                .get(col_idx)
                                .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                            {
                                Value::Null => Ok(None),
                                Value::Integer(i) => i32::try_from(*i).map(Some).map_err(|_| {
                                    SqliteError::InvalidConversion {
                                        from: Value::Integer(*i),
                                        to: DataType::Date32,
                                    }
                                }),
                                Value::Real(r) if r.fract() == 0_f64 => {
                                    let i = *r as i64;
                                    i32::try_from(i).map(Some).map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Real(*r),
                                            to: DataType::Date32,
                                        }
                                    })
                                }
                                Value::Text(t)
                                    if t.is_empty() || t.eq_ignore_ascii_case("null") =>
                                {
                                    Ok(None)
                                }
                                Value::Text(t) => {
                                    // TODO: Support other str formats
                                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                    let date =
                                        NaiveDate::parse_from_str(t, "%Y-%m-%d").map_err(|_| {
                                            SqliteError::InvalidConversion {
                                                from: Value::Text(t.clone()),
                                                to: DataType::Date32,
                                            }
                                        })?;
                                    let num_days_since_epoch =
                                        date.signed_duration_since(epoch).num_days();
                                    i32::try_from(num_days_since_epoch).map(Some).map_err(|_| {
                                        SqliteError::InvalidConversion {
                                            from: Value::Text(t.clone()),
                                            to: DataType::Date32,
                                        }
                                    })
                                }
                                v => Err(SqliteError::InvalidConversion {
                                    from: v.clone(),
                                    to: DataType::Date32,
                                }),
                            }
                        })
                        .collect::<Result<Date32Array>>()?,
                ),
                DataType::Time64(TimeUnit::Microsecond) => {
                    Arc::new(
                        data.iter()
                            .map(|row| {
                                match row
                                    .get(col_idx)
                                    .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                                {
                                    Value::Null => Ok(None),
                                    Value::Integer(i) => {
                                        let seconds_since_midnight =
                                            u32::try_from(*i).map_err(|_| {
                                                SqliteError::InvalidConversion {
                                                    from: Value::Integer(*i),
                                                    to: DataType::Time64(TimeUnit::Microsecond),
                                                }
                                            })?;
                                        // Verify it's a valid time.
                                        let _ = NaiveTime::from_num_seconds_from_midnight_opt(
                                            seconds_since_midnight,
                                            /* nsecs = */ 0,
                                        )
                                        .ok_or_else(|| SqliteError::InvalidConversion {
                                            from: Value::Integer(*i),
                                            to: DataType::Time64(TimeUnit::Microsecond),
                                        })?;
                                        Ok(Some(seconds_since_midnight as i64 * 1_000_000))
                                    }
                                    Value::Real(r) => {
                                        let seconds_since_midnight: u32 = (*r as i64)
                                            .try_into()
                                            .map_err(|_| SqliteError::InvalidConversion {
                                                from: Value::Real(*r),
                                                to: DataType::Time64(TimeUnit::Microsecond),
                                            })?;
                                        let sub_microseconds = {
                                            let fract = (r.fract() * 1_000_000_0_f64) as u32;
                                            if fract % 1_000_000_0 < 5 {
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
                                        .ok_or_else(|| SqliteError::InvalidConversion {
                                            from: Value::Real(*r),
                                            to: DataType::Time64(TimeUnit::Microsecond),
                                        })?;
                                        let microseconds_since_midnight =
                                            (seconds_since_midnight as i64 * 1_000_000)
                                                + sub_microseconds as i64;
                                        Ok(Some(microseconds_since_midnight))
                                    }
                                    Value::Text(t)
                                        if t.is_empty() || t.eq_ignore_ascii_case("null") =>
                                    {
                                        Ok(None)
                                    }
                                    Value::Text(t) => {
                                        // TODO: Support other str formats
                                        let epoch = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                                        let time = NaiveTime::parse_from_str(t, "%H:%M:%S%.f")
                                            .map_err(|_| SqliteError::InvalidConversion {
                                                from: Value::Text(t.clone()),
                                                to: DataType::Time64(TimeUnit::Microsecond),
                                            })?;
                                        let duration_since_midnight =
                                            time.signed_duration_since(epoch);
                                        Ok(Some(
                                            duration_since_midnight.num_microseconds().unwrap(),
                                        ))
                                    }
                                    v => Err(SqliteError::InvalidConversion {
                                        from: v.clone(),
                                        to: DataType::Time64(TimeUnit::Microsecond),
                                    }),
                                }
                            })
                            .collect::<Result<Time64MicrosecondArray>>()?,
                    )
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    Arc::new(
                        data.iter()
                            .map(|row| {
                                match row
                                    .get(col_idx)
                                    .ok_or_else(|| SqliteError::MissingDataForColumn(col_idx))?
                                {
                                    Value::Null => Ok(None),
                                    Value::Integer(i) => {
                                        let timestamp = NaiveDateTime::from_timestamp_opt(
                                            *i, /* nsecs = */ 0,
                                        )
                                        .ok_or_else(|| SqliteError::InvalidConversion {
                                            from: Value::Integer(*i),
                                            to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                        })?;
                                        Ok(Some(timestamp.timestamp_micros()))
                                    }
                                    Value::Real(r) => {
                                        let seconds = *r as i64;
                                        let sub_nanos = (r.fract() * 1_000_000_000_f64) as u32;
                                        let timestamp =
                                            NaiveDateTime::from_timestamp_opt(seconds, sub_nanos)
                                                .ok_or_else(|| SqliteError::InvalidConversion {
                                                from: Value::Real(*r),
                                                to: DataType::Timestamp(
                                                    TimeUnit::Microsecond,
                                                    None,
                                                ),
                                            })?;
                                        Ok(Some(timestamp.timestamp_micros()))
                                    }
                                    Value::Text(t)
                                        if t.is_empty() || t.eq_ignore_ascii_case("null") =>
                                    {
                                        Ok(None)
                                    }
                                    Value::Text(t) => {
                                        // TODO: Support other str formats
                                        let timestamp = NaiveDateTime::parse_from_str(
                                            t,
                                            "%Y-%m-%d %H:%M:%S%.f",
                                        )
                                        .map_err(|_| SqliteError::InvalidConversion {
                                            from: Value::Text(t.clone()),
                                            to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                        })?;
                                        Ok(Some(timestamp.timestamp_micros()))
                                    }
                                    v => Err(SqliteError::InvalidConversion {
                                        from: v.clone(),
                                        to: DataType::Timestamp(TimeUnit::Microsecond, None),
                                    }),
                                }
                            })
                            .collect::<Result<TimestampMicrosecondArray>>()?,
                    )
                }
                _ => unreachable!(),
            };

            cols.push(col);
        }

        Ok(RecordBatch::try_new(self.schema.clone(), cols)?)
    }
}
