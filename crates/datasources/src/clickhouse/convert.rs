use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use chrono::{DateTime, NaiveDate};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array,
    BooleanBuilder,
    Date32Builder,
    Float32Array,
    Float64Array,
    Int16Array,
    Int32Array,
    Int64Array,
    Int8Array,
    StringBuilder,
    TimestampNanosecondBuilder,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use klickhouse::block::Block;
use klickhouse::{KlickhouseError, Value};

use super::errors::Result;
use super::ClickhouseAccessState;
use crate::clickhouse::errors::ClickhouseError;

type PinnedStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send + Sync>>;

/// Convert a stream of blocks from clickhouse to a stream of record batches.
pub struct ConvertStream {
    schema: SchemaRef,
    inner: PinnedStream,
}

impl ConvertStream {
    pub fn new(schema: SchemaRef, state: Arc<ClickhouseAccessState>, query: String) -> Self {
        let schema_clone = schema.clone();
        let stream = async_stream::stream! {
            let query = state.client.query_raw(query);
            let mut stream = match query.await {
                Ok(s) => s,
                Err(e) => {
                    yield Err(DataFusionError::Execution(format!(
                        "cannot run clickhouse query: {e}",
                    )));
                    return;
                }
            };

            while let Some(block) = stream.next().await {
                let block = match block {
                    Ok(b) => b,
                    Err(e) => {
                        yield Err(DataFusionError::Execution(format!(
                            "cannot extract clickhouse block: {e}",
                        )));
                        return;
                    }
                };

                if block.rows == 0 {
                    // Empty blocks error...
                    continue;
                }

                let batch = block_to_batch(schema_clone.clone(), block)
                    .map_err(|e| DataFusionError::Execution(format!(
                        "error converting clickhouse block to record batch: {e}",
                    )));
                yield batch;
            }
        };
        let inner = Box::pin(stream);
        Self { schema, inner }
    }
}

impl Stream for ConvertStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ConvertStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Convert a block to a record batch.
fn block_to_batch(schema: Arc<Schema>, block: Block) -> Result<RecordBatch> {
    if schema.fields.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(block.rows as usize));
        return RecordBatch::try_new_with_options(schema, vec![], &options).map_err(|e| {
            ClickhouseError::String(format!("cannot create empty record batch: {e}",))
        });
    }
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
        DataType::Boolean => {
            let mut vals = BooleanBuilder::with_capacity(column.len());
            for val in column {
                match val {
                    Value::UInt8(v) => vals.append_value(v != 0),
                    Value::Null if nullable => vals.append_null(),
                    other => {
                        return Err(ClickhouseError::String(format!(
                            "unexpected value type: {other}"
                        )))
                    }
                }
            }
            Arc::new(vals.finish())
        }
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
            let mut vals = StringBuilder::with_capacity(column.len(), 10);
            for val in column {
                match val {
                    Value::String(v) => vals.append_value(String::from_utf8(v)?),
                    Value::Null if nullable => vals.append_null(),
                    other => {
                        return Err(ClickhouseError::String(format!(
                            "unexpected value type: {other}"
                        )))
                    }
                }
            }
            Arc::new(vals.finish())
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let mut vals = Date32Builder::with_capacity(column.len());
            for val in column {
                match val {
                    Value::Date(v) => vals.append_value(
                        NaiveDate::from(v).signed_duration_since(epoch).num_days() as i32,
                    ),
                    Value::Null if nullable => vals.append_null(),
                    other => {
                        return Err(ClickhouseError::String(format!(
                            "unexpected value type: {other}"
                        )))
                    }
                }
            }
            Arc::new(vals.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let mut vals = TimestampNanosecondBuilder::with_capacity(column.len());
            for val in column {
                match val {
                    Value::DateTime(v) => vals.append_value(
                        DateTime::<Tz>::try_from(v)
                            .map_err(ClickhouseError::DateTimeConvert)?
                            .timestamp_nanos_opt()
                            .unwrap(),
                    ),
                    Value::DateTime64(v) => vals.append_value(
                        DateTime::<Tz>::try_from(v)
                            .map_err(ClickhouseError::DateTimeConvert)?
                            .timestamp_nanos_opt()
                            .unwrap(),
                    ),
                    Value::Null if nullable => vals.append_null(),
                    other => {
                        return Err(ClickhouseError::String(format!(
                            "unexpected value type: {other}"
                        )))
                    }
                }
            }
            Arc::new(vals.finish().with_timezone_opt(tz))
        }
        other => {
            return Err(ClickhouseError::String(format!(
                "unhandled data type trying to convert to arrow array: {other}"
            )))
        }
    };

    Ok(arr)
}

pub struct ArrowDataType {
    pub nullable: bool,
    pub inner: DataType,
}

impl From<DataType> for ArrowDataType {
    fn from(inner: DataType) -> Self {
        Self {
            nullable: false,
            inner,
        }
    }
}

// Borrowed from klickhouse crate and modified as required.
//
// See: https://github.com/Protryon/klickhouse/blob/05b8b303f2d348961cc9a7562d39990164d4ae91/klickhouse/src/types/mod.rs#L256
pub fn clickhouse_type_to_arrow_type(
    clickhouse_type: &str,
) -> Result<ArrowDataType, KlickhouseError> {
    fn eat_identifier(input: &str) -> (&str, &str) {
        for (i, c) in input.char_indices() {
            if c.is_alphabetic() || c == '_' || c == '$' || (i > 0 && c.is_numeric()) {
                continue;
            } else {
                return (&input[..i], &input[i..]);
            }
        }
        (input, "")
    }

    fn parse_args(input: &str) -> Result<Vec<&str>, KlickhouseError> {
        if !input.starts_with('(') || !input.ends_with(')') {
            return Err(KlickhouseError::TypeParseError(
                "malformed arguments to type".to_string(),
            ));
        }
        let input = input[1..input.len() - 1].trim();
        let mut out = vec![];
        let mut in_parens = 0usize;
        let mut last_start = 0;
        // todo: handle parens in enum strings?
        for (i, c) in input.char_indices() {
            match c {
                ',' => {
                    if in_parens == 0 {
                        out.push(input[last_start..i].trim());
                        last_start = i + 1;
                    }
                }
                '(' => {
                    in_parens += 1;
                }
                ')' => {
                    in_parens -= 1;
                }
                _ => (),
            }
        }
        if in_parens != 0 {
            return Err(KlickhouseError::TypeParseError(
                "mismatched parenthesis".to_string(),
            ));
        }
        if last_start != input.len() {
            out.push(input[last_start..input.len()].trim());
        }
        Ok(out)
    }

    fn parse_scale(from: &str) -> Result<usize, KlickhouseError> {
        from.parse()
            .map_err(|_| KlickhouseError::TypeParseError("couldn't parse scale".to_string()))
    }

    fn parse_precision(from: &str) -> Result<usize, KlickhouseError> {
        from.parse()
            .map_err(|_| KlickhouseError::TypeParseError("couldn't parse precision".to_string()))
    }

    let (ident, following) = eat_identifier(clickhouse_type);
    if ident.is_empty() {
        return Err(KlickhouseError::TypeParseError(format!(
            "invalid empty identifier for type: '{}'",
            clickhouse_type,
        )));
    }
    let following = following.trim();
    if !following.is_empty() {
        let args = parse_args(following)?;
        return Ok(match ident {
            "Decimal" => {
                if args.len() != 2 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Decimal, expected 2 and got {}",
                        args.len()
                    )));
                }
                let p = parse_precision(args[0])?;
                let s = parse_scale(args[1])?;
                if p <= 38 {
                    DataType::Decimal128(p as u8, s as i8).into()
                } else if p <= 76 {
                    DataType::Decimal256(p as u8, s as i8).into()
                } else {
                    return Err(KlickhouseError::TypeParseError(
                        "bad decimal spec, cannot exceed 76 precision".to_string(),
                    ));
                }
            }
            "Decimal32" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Decimal32, expected 1 and got {}",
                        args.len()
                    )));
                }
                let s = parse_scale(args[0])?;
                DataType::Decimal128(9, s as i8).into()
            }
            "Decimal64" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Decimal64, expected 1 and got {}",
                        args.len()
                    )));
                }
                let s = parse_scale(args[0])?;
                DataType::Decimal128(18, s as i8).into()
            }
            "Decimal128" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Decimal128, expected 1 and got {}",
                        args.len()
                    )));
                }
                let s = parse_scale(args[0])?;
                DataType::Decimal128(38, s as i8).into()
            }
            "Decimal256" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Decimal256, expected 1 and got {}",
                        args.len()
                    )));
                }
                let s = parse_scale(args[0])?;
                DataType::Decimal256(76, s as i8).into()
            }
            "FixedString" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for FixedString, expected 1 and got {}",
                        args.len()
                    )));
                }
                let _s = parse_scale(args[0])?;
                DataType::Utf8.into()
            }
            "DateTime" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for DateTime, expected 1 and got {}",
                        args.len()
                    )));
                }
                if !args[0].starts_with('\'') || !args[0].ends_with('\'') {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "failed to parse timezone for DateTime: '{}'",
                        args[0]
                    )));
                }
                let tz = &args[0][1..args[0].len() - 1];
                // TODO: This is technically "second" precision.
                DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())).into()
            }
            "DateTime64" => {
                if args.len() == 2 {
                    if !args[1].starts_with('\'') || !args[1].ends_with('\'') {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "failed to parse timezone for DateTime64: '{}'",
                            args[0]
                        )));
                    }
                    let p = parse_precision(args[0])?;
                    // TODO: Use the actual precision.
                    let _tu = if p < 3 {
                        TimeUnit::Second
                    } else if p < 6 {
                        TimeUnit::Millisecond
                    } else if p < 9 {
                        TimeUnit::Microsecond
                    } else {
                        TimeUnit::Nanosecond
                    };
                    let tz = &args[1][1..args[1].len() - 1];
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())).into()
                } else if args.len() == 1 {
                    let p = parse_precision(args[0])?;
                    // TODO: Use the actual precision.
                    let _tu = if p < 3 {
                        TimeUnit::Second
                    } else if p < 6 {
                        TimeUnit::Millisecond
                    } else if p < 9 {
                        TimeUnit::Microsecond
                    } else {
                        TimeUnit::Nanosecond
                    };
                    DataType::Timestamp(TimeUnit::Nanosecond, None).into()
                } else {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for DateTime64, expected 1 or 2 and got {}",
                        args.len()
                    )));
                }
            }
            "Enum8" => {
                return Err(KlickhouseError::TypeParseError(
                    "unsupported Enum8 type".to_string(),
                ));
            }
            "Enum16" => {
                return Err(KlickhouseError::TypeParseError(
                    "unsupported Enum16 type".to_string(),
                ));
            }
            "LowCardinality" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for LowCardinality, expected 1 and got {}",
                        args.len()
                    )));
                }
                return Err(KlickhouseError::TypeParseError(
                    "unsupported LowCardinality type".to_string(),
                ));
            }
            "Array" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Array, expected 1 and got {}",
                        args.len()
                    )));
                }
                return Err(KlickhouseError::TypeParseError(
                    "unsupported Array type".to_string(),
                ));
            }
            "Nested" => {
                return Err(KlickhouseError::TypeParseError(
                    "unsupported Nested type".to_string(),
                ));
            }
            "Tuple" => {
                // let mut inner = vec![];
                // for arg in args {
                //     inner.push(arg.trim().parse()?);
                // }
                return Err(KlickhouseError::TypeParseError(
                    "unsupported Tuple type".to_string(),
                ));
            }
            "Nullable" => {
                if args.len() != 1 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Nullable, expected 1 and got {}",
                        args.len()
                    )));
                }
                let dt = clickhouse_type_to_arrow_type(args[0])?;
                ArrowDataType {
                    nullable: true,
                    inner: dt.inner,
                }
            }
            "Map" => {
                if args.len() != 2 {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "bad arg count for Map, expected 2 and got {}",
                        args.len()
                    )));
                }
                return Err(KlickhouseError::TypeParseError(
                    "unsupported Map type".to_string(),
                ));
            }
            _ => {
                return Err(KlickhouseError::TypeParseError(format!(
                    "invalid type with arguments: '{}'",
                    ident
                )))
            }
        });
    }
    Ok(match ident {
        "Bool" => DataType::Boolean.into(),
        "Int8" => DataType::Int8.into(),
        "Int16" => DataType::Int16.into(),
        "Int32" => DataType::Int32.into(),
        "Int64" => DataType::Int64.into(),
        "Int128" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported Int128 type".to_string(),
            ))
        }
        "Int256" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported Int256 type".to_string(),
            ))
        }
        "UInt8" => DataType::UInt8.into(),
        "UInt16" => DataType::UInt16.into(),
        "UInt32" => DataType::UInt32.into(),
        "UInt64" => DataType::UInt64.into(),
        "UInt128" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported UInt128 type".to_string(),
            ))
        }
        "UInt256" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported UInt256 type".to_string(),
            ))
        }
        "Float32" => DataType::Float32.into(),
        "Float64" => DataType::Float64.into(),
        "String" => DataType::Utf8.into(),
        "UUID" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported UUID type".to_string(),
            ))
        }
        "Date" => DataType::Date32.into(),
        "Date32" => {
            // Unlike Boolean, klickhouse doesn't parse Date32 values and
            // returns empty batches (basically returning no data at all :/).
            return Err(KlickhouseError::TypeParseError(
                "unsupported Date32 type".to_string(),
            ));
        }
        "DateTime" => DataType::Timestamp(TimeUnit::Nanosecond, None).into(),
        "IPv4" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported IPv4 type".to_string(),
            ))
        }
        "IPv6" => {
            return Err(KlickhouseError::TypeParseError(
                "unsupported IPv6 type".to_string(),
            ))
        }
        _ => {
            return Err(KlickhouseError::TypeParseError(format!(
                "invalid type name: '{}'",
                ident
            )))
        }
    })
}
