use std::{collections::HashMap, io::Cursor, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
            Float64Builder, Int16Array, Int32Array, Int64Array, Int64Builder, Int8Array,
            StringBuilder, StructArray, Time64NanosecondBuilder, TimestampNanosecondBuilder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        error::ArrowError,
        ipc::reader::StreamReader,
        record_batch::RecordBatch,
    },
    scalar::ScalarValue,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::Session,
    datatype::SnowflakeDataType,
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
pub struct QueryBindParameter {
    #[serde(rename = "type")]
    typ: SnowflakeDataType,
    value: String,
}

impl QueryBindParameter {
    fn new<S: ToString>(typ: SnowflakeDataType, val: S) -> Self {
        QueryBindParameter {
            typ,
            value: val.to_string(),
        }
    }

    pub fn new_text<S: ToString>(val: S) -> Self {
        Self::new(SnowflakeDataType::Text, val)
    }
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryDataRowType {
    name: String,

    #[serde(rename = "type")]
    typ: SnowflakeDataType,

    precision: Option<i64>,
    scale: Option<i64>,

    nullable: bool,
}

pub fn snowflake_to_arrow_datatype(
    dt: SnowflakeDataType,
    precision: Option<i64>,
    scale: Option<i64>,
) -> DataType {
    use SnowflakeDataType as Dt;
    match dt {
        Dt::Binary => DataType::Binary,
        Dt::Boolean => DataType::Boolean,
        Dt::Any | Dt::Array | Dt::Object | Dt::Char | Dt::Text | Dt::Variant => DataType::Utf8,
        Dt::Real => DataType::Float64,
        Dt::Fixed | Dt::Number => {
            let (precision, scale) = (precision.unwrap() as u8, scale.unwrap() as i8);
            DataType::Decimal128(precision, scale)
        }
        Dt::Date => DataType::Date32,
        Dt::Time => DataType::Time64(TimeUnit::Nanosecond),
        Dt::Timestamp | Dt::TimestampNtz => DataType::Timestamp(TimeUnit::Nanosecond, None),
        Dt::TimestampTz | Dt::TimestampLtz => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
    }
}

fn snowflake_to_arrow_schema(rowtype: Vec<QueryDataRowType>) -> Schema {
    let mut fields = Vec::new();
    for r in rowtype.into_iter() {
        let datatype = snowflake_to_arrow_datatype(r.typ, r.precision, r.scale);
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

impl QueryData {
    fn json_to_arrow(schema: SchemaRef, rows: Vec<Vec<Option<String>>>) -> Result<RecordBatchIter> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields.len());
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

        let record_batch = RecordBatch::try_new(schema, columns)?;
        Ok(RecordBatchIter::Exact(Some(record_batch)))
    }

    fn ipc_to_arrow(schema: SchemaRef, rowset_base64: String) -> Result<RecordBatchIter> {
        if rowset_base64.is_empty() {
            // Return an empty record batch in case of empty result
            Ok(RecordBatchIter::Exact(Some(RecordBatch::new_empty(schema))))
        } else {
            let mut buf = Vec::new();
            base64_engine.decode_vec(rowset_base64, &mut buf)?;

            let buf = Cursor::new(buf);
            let reader = StreamReader::try_new(buf, None)?;
            Ok(RecordBatchIter::Stream { reader, schema })
        }
    }
}

pub enum RecordBatchIter {
    Stream {
        reader: StreamReader<Cursor<Vec<u8>>>,
        schema: SchemaRef,
    },
    Exact(Option<RecordBatch>),
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

impl RecordBatchIter {
    fn normalize_column(expected_field: &Field, actual_field: &Field, col: &ArrayRef) -> ArrayRef {
        match (expected_field.data_type(), actual_field.data_type()) {
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
        }
    }

    fn next_batch(
        schema: SchemaRef,
        batch: Result<RecordBatch, ArrowError>,
    ) -> Result<RecordBatch> {
        let batch = batch?;
        let actual_schema = batch.schema();
        let mut columns = Vec::with_capacity(schema.fields.len());
        for (col_idx, (expected_field, actual_field)) in schema
            .fields
            .iter()
            .zip(actual_schema.fields.iter())
            .enumerate()
        {
            let col = batch.column(col_idx);
            let col = Self::normalize_column(expected_field, actual_field, col);
            columns.push(col);
        }

        let batch = RecordBatch::try_new(schema, columns)?;
        Ok(batch)
    }
}

impl Iterator for RecordBatchIter {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Exact(batch) => Ok(batch.take()).transpose(),
            Self::Stream { reader, schema } => reader
                .next()
                .map(|batch| Self::next_batch(schema.clone(), batch)),
        }
    }
}

pub struct QueryResultChunk {
    schema: SchemaRef,
    iter: RecordBatchIter,
}

impl TryFrom<QueryData> for QueryResultChunk {
    type Error = SnowflakeError;

    fn try_from(data: QueryData) -> Result<Self, Self::Error> {
        let schema = Arc::new(snowflake_to_arrow_schema(
            data.rowtype.expect("rowtype should exist in query result"),
        ));

        let query_result_format = data
            .query_result_format
            .expect("query_result_format should exist in query result");

        let iter = match query_result_format {
            QueryResultFormat::Json => {
                let rows = data
                    .rowset
                    .expect("rowset should exist in query result for json format");
                QueryData::json_to_arrow(schema.clone(), rows)?
            }
            QueryResultFormat::Arrow => {
                let rowset_base64 = data.rowset_base64.unwrap_or(String::new());
                QueryData::ipc_to_arrow(schema.clone(), rowset_base64)?
            }
        };

        Ok(Self { schema, iter })
    }
}

impl QueryResultChunk {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn into_row_iter(self) -> QueryResultRowIter {
        QueryResultRowIter {
            schema: self.schema,
            iter: self.iter,
            curr_batch: None,
            curr_row: 0,
            initialized: false,
        }
    }
}

impl IntoIterator for QueryResultChunk {
    type Item = Result<RecordBatch>;
    type IntoIter = RecordBatchIter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

pub struct QueryResultRowIter {
    schema: SchemaRef,
    iter: RecordBatchIter,
    curr_batch: Option<RecordBatch>,
    curr_row: usize,
    // If the iteration has been initialized. This happens after the first call
    // to `iter.next()`.
    initialized: bool,
}

impl QueryResultRowIter {
    fn try_next_batch(&mut self) -> Result<()> {
        while let Some(curr_batch) = self.curr_batch.as_ref() {
            if self.curr_row < curr_batch.num_rows() {
                break;
            }
            self.curr_batch = self.iter.next().transpose()?;
            self.curr_row = 0;
        }
        Ok(())
    }

    fn next_row(&mut self) -> Result<Option<QueryResultRow>> {
        if !self.initialized {
            // Get the first batch when `next()` is called for the first time.
            self.curr_batch = self.iter.next().transpose()?;
            self.initialized = true;
        }

        // Try getting the next batch if required (i.e., the current one is
        // exhausted).
        self.try_next_batch()?;

        // Here we definitely know if there exists a batch or not.
        let curr_batch = if let Some(curr_batch) = self.curr_batch.as_ref() {
            curr_batch
        } else {
            return Ok(None);
        };

        // Advance row.
        let row = self.curr_row;
        self.curr_row += 1;

        Ok(Some(QueryResultRow {
            schema: self.schema.clone(),
            row,
            cols: curr_batch.columns().to_vec(),
        }))
    }
}

impl Iterator for QueryResultRowIter {
    type Item = Result<QueryResultRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_row().transpose()
    }
}

pub struct QueryResultRow {
    schema: SchemaRef,
    row: usize,
    cols: Vec<ArrayRef>,
}

impl QueryResultRow {
    pub fn get_column(&self, col_idx: usize) -> Option<Result<ScalarValue>> {
        fn get_scalar(col: &ArrayRef, row_idx: usize) -> Result<ScalarValue> {
            let scalar = ScalarValue::try_from_array(&col, row_idx)?;
            Ok(scalar)
        }
        self.cols.get(col_idx).map(|col| get_scalar(col, self.row))
    }

    pub fn get_column_by_name(&self, col_name: &str) -> Option<Result<ScalarValue>> {
        let (col_idx, _) = self.schema.column_with_name(col_name)?;
        self.get_column(col_idx)
    }
}

pub struct Query {
    pub sql: String,
    pub bindings: Vec<QueryBindParameter>,
}

impl Query {
    pub async fn exec(
        self,
        client: &SnowflakeClient,
        session: &Session,
    ) -> Result<QueryResultChunk> {
        if !session.token.is_valid() {
            // TODO: session.refresh_token()
            // For now just let the query go and return the error from the
            // snowflake server.
        }

        let bindings = if self.bindings.is_empty() {
            None
        } else {
            let bindings: HashMap<_, _> = self
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    let k = (i + 1).to_string();
                    (k, b)
                })
                .collect();
            Some(bindings)
        };

        let res: QueryResponse = client
            .execute(
                QUERY_ENDPOINT,
                &QueryParams {
                    request_id: RequestId::new(),
                },
                &QueryBody {
                    sql_text: self.sql,
                    bindings,
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
