use std::{collections::HashMap, io::Cursor, sync::Arc};

use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Float64Builder, Int16Array, Int32Array, Int64Array, Int64Builder, Int8Array, StringBuilder,
        StructArray, Time64NanosecondBuilder, TimestampNanosecondBuilder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::Session,
    errors::{Result, SnowflakeError},
    req::{RequestId, SnowflakeClient},
};

use base64::{engine::general_purpose::STANDARD as base64_engine, Engine};

const QUERY_ENDPOINT: &str = "/queries/v1/query-request";

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum QueryResultFormat {
    Json,
    Arrow,
}

#[derive(Debug, Serialize)]
struct QueryBindParameter {
    #[serde(rename = "type")]
    r#type: String,
    value: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct QueryBodyParameters {
    go_query_result_format: QueryResultFormat,
}

impl Default for QueryBodyParameters {
    fn default() -> Self {
        Self {
            go_query_result_format: QueryResultFormat::Arrow,
        }
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryBody {
    sql_text: String,
    async_exec: bool,
    sequence_id: u64,
    is_internal: bool,
    describe_only: bool,
    parameters: QueryBodyParameters,

    #[serde(skip_serializing_if = "Option::is_none")]
    bindings: Option<HashMap<String, QueryBindParameter>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    bind_stage: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryParams {
    request_id: RequestId,
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    data: QueryData,
    message: Option<String>,
    code: Option<String>,
    success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryData {
    #[allow(unused)]
    total: Option<i64>,
    #[allow(unused)]
    returned: Option<i64>,
    #[allow(unused)]
    query_id: Option<String>,
    // TODO: A lot more other fields...
    rowtype: Option<Vec<QueryDataRowType>>,

    rowset: Option<Vec<Vec<Option<String>>>>,
    rowset_base64: Option<String>,

    query_result_format: Option<QueryResultFormat>,
}

fn snowflake_to_arrow_schema(rowtype: Vec<QueryDataRowType>) -> Schema {
    let mut fields = Vec::new();
    for r in rowtype.into_iter() {
        let datatype = match r.r#type.to_lowercase().as_str() {
            "binary" => DataType::Binary,
            "boolean" => DataType::Boolean,
            "any" | "array" | "object" | "char" | "text" | "variant" => DataType::Utf8,
            "real" => DataType::Float64,
            "fixed" => {
                let (precision, scale) = (r.precision.unwrap() as u8, r.scale.unwrap() as i8);
                DataType::Decimal128(precision, scale)
            }
            "date" => DataType::Date32,
            "time" => DataType::Time64(TimeUnit::Nanosecond),
            "timestamp" | "timestamp_ntz" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            "timestamp_tz" | "timestamp_ltz" => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string()))
            }
            dt => unreachable!("programming error: snowflake datatype '{dt}' isn't implemented"),
        };
        let field = Field::new(r.name, datatype, r.nullable);
        fields.push(field);
    }
    Schema::new(fields)
}

macro_rules! make_json_column_using {
    ($arr:expr, $rows:expr, $col_idx:expr, $parse_fn:expr) => {{
        for row in $rows.iter() {
            let val = row.get($col_idx).expect("value for column should exist");
            let val = match val {
                Some(s) => Some($parse_fn(s)),
                None => None,
            };
            $arr.append_option(val);
        }
        Arc::new($arr.finish())
    }};
}

macro_rules! make_json_column {
    ($builder:ty, $rows:expr, $col_idx:expr, $parse_fn:expr) => {{
        let mut arr = <$builder>::with_capacity($rows.len());
        make_json_column_using!(arr, $rows, $col_idx, $parse_fn)
    }};
}

macro_rules! make_ipc_column {
    ($col:expr, $from_arr:ty, $map_fn:expr, $builder:ty, $dt:expr) => {{
        let rows: &$from_arr = $col.as_any().downcast_ref().unwrap();
        let mut arr = <$builder>::with_capacity(rows.len()).with_data_type($dt.clone());
        rows.iter().for_each(|row| {
            let row = row.map($map_fn);
            arr.append_option(row)
        });
        Arc::new(arr.finish())
    }};
}

impl QueryData {
    fn json_to_arrow(self) -> Result<RecordBatch> {
        let schema =
            snowflake_to_arrow_schema(self.rowtype.expect("rowtype should exist in query result"));
        let fields = &schema.fields;

        let rows = self.rowset.expect("rowset should exist in query result");

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (col_idx, field) in schema.fields.iter().enumerate() {
            let col: ArrayRef = match field.data_type() {
                DataType::Boolean => {
                    make_json_column!(BooleanBuilder, rows, col_idx, |s| s == "1")
                }
                DataType::Utf8 => {
                    let mut arr = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                    make_json_column_using!(arr, rows, col_idx, String::as_str)
                }
                DataType::Binary => {
                    fn parse_binary(s: &String) -> Vec<u8> {
                        hex::decode(s).expect("value should be a valid hex representation")
                    }
                    let mut arr = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                    make_json_column_using!(arr, rows, col_idx, parse_binary)
                }
                DataType::Float64 => {
                    fn parse_float(s: &str) -> f64 {
                        s.parse()
                            .expect("value should be a valid floating point string representation")
                    }
                    make_json_column!(Float64Builder, rows, col_idx, parse_float)
                }
                DataType::Int64 => {
                    fn parse_int(s: &str) -> i64 {
                        s.parse().expect("value should be a valid i64")
                    }
                    make_json_column!(Int64Builder, rows, col_idx, parse_int)
                }
                dt @ DataType::Decimal128(_, _) => {
                    fn parse_decimal(s: &str) -> i128 {
                        let d: rust_decimal::Decimal = s
                            .parse()
                            .expect("value should be a valid decimal representation");
                        d.mantissa()
                    }
                    let mut arr =
                        Decimal128Builder::with_capacity(rows.len()).with_data_type(dt.clone());
                    make_json_column_using!(arr, rows, col_idx, parse_decimal)
                }
                DataType::Date32 => {
                    fn parse_date(s: &str) -> i32 {
                        s.parse()
                            .expect("value should be a valid i32 (number of days)")
                    }
                    make_json_column!(Date32Builder, rows, col_idx, parse_date)
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    fn parse_time(s: &str) -> i64 {
                        let times: Vec<i64> = s
                            .splitn(2, '.')
                            .map(|v| {
                                v.parse().expect(
                                    "value should be a valid time of format <seconds>.<nanos>",
                                )
                            })
                            .collect();

                        let (seconds, nanos) = (times[0], times[1]);
                        (seconds * 1_000_000_000) + nanos
                    }
                    make_json_column!(Time64NanosecondBuilder, rows, col_idx, parse_time)
                }
                dt @ DataType::Timestamp(TimeUnit::Nanosecond, _tz) => {
                    fn parse_timestamp(s: &str) -> i64 {
                        let times: Vec<i64> = s
                            .splitn(2, '.')
                            .map(|v| {
                                v.parse().expect(
                                    "value should be a valid timestamp of format <seconds since epoch>.<nanos>",
                                )
                            })
                            .collect();

                        let (seconds, nanos) = (times[0], times[1]);
                        (seconds * 1_000_000_000) + nanos
                    }
                    let mut arr = TimestampNanosecondBuilder::with_capacity(rows.len())
                        .with_data_type(dt.clone());
                    make_json_column_using!(arr, rows, col_idx, parse_timestamp)
                }
                dt => unreachable!(
                    "programming error: invalid arrow datatype '{dt}' from json result"
                ),
            };
            columns.push(col);
        }

        let record_batch = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(record_batch)
    }

    fn ipc_to_arrow(self) -> Result<RecordBatch> {
        let mut buf = Vec::new();
        base64_engine.decode_vec(
            self.rowset_base64
                .expect("rowset_base64 should exist in query result"),
            &mut buf,
        )?;
        let mut buf = Cursor::new(buf);
        let reader = StreamReader::try_new(&mut buf, None)?;
        let batch = reader
            .into_iter()
            .next()
            .expect("ipc stream reader iterator should return at-least one batch")?;

        let expected_schema =
            snowflake_to_arrow_schema(self.rowtype.expect("rowtype should exist in query result"));
        let actual_schema = batch.schema();

        let mut columns = Vec::with_capacity(expected_schema.fields.len());

        for (col_idx, (expected_field, actual_field)) in expected_schema
            .fields
            .iter()
            .zip(actual_schema.fields.iter())
            .enumerate()
        {
            let col = batch.column(col_idx);

            let col: ArrayRef = match (expected_field.data_type(), actual_field.data_type()) {
                (dt @ DataType::Decimal128(_, _), DataType::Int8) => {
                    make_ipc_column!(col, Int8Array, |r| r as i128, Decimal128Builder, dt)
                }
                (dt @ DataType::Decimal128(_, _), DataType::Int16) => {
                    make_ipc_column!(col, Int16Array, |r| r as i128, Decimal128Builder, dt)
                }
                (dt @ DataType::Decimal128(_, _), DataType::Int32) => {
                    make_ipc_column!(col, Int32Array, |r| r as i128, Decimal128Builder, dt)
                }
                (dt @ DataType::Decimal128(_, _), DataType::Int64) => {
                    make_ipc_column!(col, Int64Array, |r| r as i128, Decimal128Builder, dt)
                }
                (dt @ DataType::Time64(TimeUnit::Nanosecond), DataType::Int64) => {
                    make_ipc_column!(col, Int64Array, |r| r, Time64NanosecondBuilder, dt)
                }
                (dt @ DataType::Timestamp(TimeUnit::Nanosecond, _tz), DataType::Struct(_)) => {
                    let rows: &StructArray = col.as_any().downcast_ref().unwrap();
                    let seconds = rows
                        .column_by_name("epoch")
                        .expect("column 'epoch' should exist in time struct")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let nanos = rows
                        .column_by_name("fraction")
                        .expect("column 'fraction' should exist in time struct")
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let mut arr = TimestampNanosecondBuilder::with_capacity(rows.len())
                        .with_data_type(dt.clone());
                    (0..rows.len()).for_each(|row_idx| {
                        if rows.is_null(row_idx) {
                            arr.append_null();
                        } else {
                            let t = seconds.value(row_idx) * 1_000_000_000;
                            let t = t + nanos.value(row_idx) as i64;
                            arr.append_value(t);
                        }
                    });
                    Arc::new(arr.finish())
                }
                (expected, actual) if expected == actual => Arc::clone(col),
                (expected, actual) => unreachable!(
                    "programming error: conversion from '{actual}' to '{expected}' not supported for snowflake"
                ),
            };

            columns.push(col);
        }

        // let batch = RecordBatch::try_new(actual_schema, columns)?;
        let batch = RecordBatch::try_new(Arc::new(expected_schema), columns)?;
        Ok(batch)
    }
}

impl TryFrom<QueryData> for RecordBatch {
    type Error = SnowflakeError;

    fn try_from(data: QueryData) -> Result<Self, Self::Error> {
        match &data
            .query_result_format
            .expect("query_result_format should exist in query result")
        {
            QueryResultFormat::Json => data.json_to_arrow(),
            QueryResultFormat::Arrow => data.ipc_to_arrow(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryDataRowType {
    name: String,

    #[serde(rename = "type")]
    r#type: String,

    precision: Option<i64>,
    scale: Option<i64>,

    nullable: bool,
}

pub struct Query {
    pub sql: String,
}

impl Query {
    pub async fn exec(self, client: &SnowflakeClient, session: &Session) -> Result<RecordBatch> {
        if !session.token.is_valid() {
            // TODO: session.refresh_token()
            // For now just let the query go and return the error from the
            // snowflake server.
        }

        let res: QueryResponse = client
            .execute(
                QUERY_ENDPOINT,
                &QueryParams {
                    request_id: RequestId::new(),
                },
                &QueryBody {
                    sql_text: self.sql,
                    ..Default::default()
                },
                Some(&session.token),
            )
            .await?;

        if !res.success {
            return Err(SnowflakeError::QueryError {
                code: res.code.unwrap_or_default(),
                message: res.message.unwrap_or_default(),
            });
        }

        // TODO: Check if total == returned else fetch more?
        res.data.try_into()
    }
}
