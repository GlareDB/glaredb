use chrono::{DateTime, NaiveDate};
use chrono_tz::Tz;
use datafusion::{
    arrow::{array::Date32Array, datatypes::Field},
    error::DataFusionError,
};
use datafusion::{
    arrow::{
        array::{
            Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
            StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
            UInt8Array,
        },
        datatypes::{DataType, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    physical_plan::RecordBatchStream,
};
use futures::{Stream, StreamExt};
use klickhouse::{block::Block, KlickhouseError, Type, Value};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::clickhouse::errors::ClickhouseError;

use super::errors::Result;

/// Convert a stream of blocks from clickhouse to a stream of record batches.
pub struct ConvertStream {
    pub schema: Arc<Schema>,
    pub inner: Pin<Box<dyn Stream<Item = Result<Block, KlickhouseError>> + Send + 'static>>,
}

impl Stream for ConvertStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(block))) => {
                    // Clickhouse can return empty blocks. Try polling for the
                    // next block in that case.
                    if block.rows == 0 {
                        continue;
                    }

                    return Poll::Ready(Some(
                        block_to_batch(self.schema.clone(), block)
                            .map_err(|e| DataFusionError::Execution(e.to_string())),
                    ));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(DataFusionError::Execution(e.to_string()))))
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for ConvertStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Produce an arrow schema based on the type found in a block.
pub fn block_to_schema(block: Block) -> Result<Schema> {
    let mut fields = Vec::with_capacity(block.column_types.len());

    for (name, col_type) in block.column_types.iter() {
        let (arrow_typ, nullable) = match col_type {
            Type::Nullable(typ) => (type_to_datatype(typ)?, true),
            typ => (type_to_datatype(typ)?, false),
        };

        let field = Field::new(name, arrow_typ, nullable);
        fields.push(field);
    }

    Ok(Schema::new(fields))
}

/// Convert a clickhouse sql type to an arrow data type.
///
/// Clickhouse type reference: <https://clickhouse.com/docs/en/sql-reference/data-types>
fn type_to_datatype(typ: &Type) -> Result<DataType> {
    // Note no bools, see <https://github.com/Protryon/klickhouse/issues/25> for
    // klickhouse author's response to this.
    Ok(match typ {
        Type::UInt8 => DataType::UInt8,
        Type::UInt16 => DataType::UInt16,
        Type::UInt32 => DataType::UInt32,
        Type::UInt64 => DataType::UInt64,
        Type::Int8 => DataType::Int8,
        Type::Int16 => DataType::Int16,
        Type::Int32 => DataType::Int32,
        Type::Int64 => DataType::Int64,
        Type::Float32 => DataType::Float32,
        Type::Float64 => DataType::Float64,
        Type::String | Type::FixedString(_) => DataType::Utf8,
        // Clickhouse has both a 'Date' type (2 bytes) and a
        // 'Date32' type (4 bytes). The library doesn't support
        // 'Date64' type yet.
        // TODO: Maybe upstream support for Date64?
        Type::Date => DataType::Date32,
        Type::DateTime(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        Type::DateTime64(_precision, tz) => {
            // We store all timestamps in nanos, so the precision
            // here doesn't matter.
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.to_string().into()))
        }
        other => {
            return Err(ClickhouseError::String(format!(
                "unsupported Clickhouse type: {other:?}"
            )))
        }
    })
}

/// Convert a block to a record batch.
fn block_to_batch(schema: Arc<Schema>, block: Block) -> Result<RecordBatch> {
    if schema.fields.len() != block.column_data.len() {
        return Err(ClickhouseError::String(format!(
            "expected {} columns, got {}",
            schema.fields.len(),
            block.column_data.len()
        )));
    }

    let mut arrs = Vec::with_capacity(schema.fields.len());

    for (field, col) in schema.fields.iter().zip(block.column_data.into_values()) {
        let arr = column_to_array(field.data_type().clone(), col, field.is_nullable())?;
        arrs.push(arr);
    }

    let batch = RecordBatch::try_new(schema, arrs)?;
    let batch = crate::common::util::normalize_batch(&batch)?;
    Ok(batch)
}

/// Converts a column from a block into an arrow array.
///
/// The column's data type should be known beforehand.
fn column_to_array(
    datatype: DataType,
    column: Vec<Value>,
    nullable: bool,
) -> Result<Arc<dyn Array>> {
    // TODO: This could be a function, but I'm not too keen on figuring out the
    // types right now.
    macro_rules! make_primitive_array {
        ($value_variant:ident, $arr_type:ty, $nullable:expr) => {{
            if nullable {
                let mut vals = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::$value_variant(v) => vals.push(Some(v)),
                        Value::Null if nullable => vals.push(None),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(<$arr_type>::from(vals))
            } else {
                let mut vals = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::$value_variant(v) => vals.push(v),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(<$arr_type>::from(vals))
            }
        }};
    }

    let arr: Arc<dyn Array> = match datatype {
        DataType::UInt8 => make_primitive_array!(UInt8, UInt8Array, nullable),
        DataType::UInt16 => make_primitive_array!(UInt16, UInt16Array, nullable),
        DataType::UInt32 => make_primitive_array!(UInt32, UInt32Array, nullable),
        DataType::UInt64 => make_primitive_array!(UInt64, UInt64Array, nullable),
        DataType::Int8 => make_primitive_array!(Int8, Int8Array, nullable),
        DataType::Int16 => make_primitive_array!(Int16, Int16Array, nullable),
        DataType::Int32 => make_primitive_array!(Int32, Int32Array, nullable),
        DataType::Int64 => make_primitive_array!(Int64, Int64Array, nullable),
        DataType::Float32 => make_primitive_array!(Float32, Float32Array, nullable),
        DataType::Float64 => make_primitive_array!(Float64, Float64Array, nullable),
        DataType::Utf8 => {
            if nullable {
                let mut vals = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::String(v) => vals.push(Some(String::from_utf8(v)?)),
                        Value::Null if nullable => vals.push(None),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(StringArray::from(vals))
            } else {
                let mut vals = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::String(v) => vals.push(String::from_utf8(v)?),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(StringArray::from(vals))
            }
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            if nullable {
                let mut vals: Vec<Option<NaiveDate>> = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::Date(v) => vals.push(Some(v.into())),
                        Value::Null if nullable => vals.push(None),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(Date32Array::from(
                    vals.into_iter()
                        .map(|date| {
                            date.map(|date| date.signed_duration_since(epoch).num_days() as i32)
                        })
                        .collect::<Vec<_>>(),
                ))
            } else {
                let mut vals: Vec<NaiveDate> = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::Date(v) => vals.push(v.into()),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(Date32Array::from(
                    vals.into_iter()
                        .map(|date| date.signed_duration_since(epoch).num_days() as i32)
                        .collect::<Vec<_>>(),
                ))
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            if nullable {
                let mut vals: Vec<Option<DateTime<Tz>>> = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::DateTime(v) => vals.push(Some(
                            v.try_into().map_err(ClickhouseError::DateTimeConvert)?,
                        )),
                        Value::DateTime64(v) => vals.push(Some(
                            v.try_into().map_err(ClickhouseError::DateTimeConvert)?,
                        )),
                        Value::Null if nullable => vals.push(None),
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(
                    TimestampNanosecondArray::from(
                        vals.into_iter()
                            .map(|time| time.map(|time| time.timestamp_nanos_opt().unwrap()))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone_opt(tz),
                )
            } else {
                let mut vals: Vec<DateTime<Tz>> = Vec::with_capacity(column.len());
                for val in column {
                    match val {
                        Value::DateTime(v) => {
                            vals.push(v.try_into().map_err(ClickhouseError::DateTimeConvert)?)
                        }
                        Value::DateTime64(v) => {
                            vals.push(v.try_into().map_err(ClickhouseError::DateTimeConvert)?)
                        }
                        other => {
                            return Err(ClickhouseError::String(format!(
                                "unexpected value type: {other}"
                            )))
                        }
                    }
                }
                Arc::new(
                    TimestampNanosecondArray::from(
                        vals.into_iter()
                            .map(|time| time.timestamp_nanos_opt().unwrap())
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone_opt(tz),
                )
            }
        }
        other => {
            return Err(ClickhouseError::String(format!(
                "unhandled data type trying to convert to arrow array: {other}"
            )))
        }
    };

    Ok(arr)
}
