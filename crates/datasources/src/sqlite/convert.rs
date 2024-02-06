use std::sync::Arc;

use async_sqlite::rusqlite::types::Value;
use datafusion::arrow::array::{
    Array,
    BinaryBuilder,
    BooleanArray,
    Float64Array,
    Int64Array,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
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
                                    builder.append_null()
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
                _ => unreachable!(),
            };

            cols.push(col);
        }

        Ok(RecordBatch::try_new(self.schema.clone(), cols)?)
    }
}
