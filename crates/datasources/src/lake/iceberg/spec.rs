use crate::lake::iceberg::errors::{IcebergError, Result};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub enum FileFormat {
    Parquet,
    Orc,
    Avro,
}

// boolean	True or false
// int	32-bit signed integers	Can promote to long
// long	64-bit signed integers
// float	32-bit IEEE 754 floating point	Can promote to double
// double	64-bit IEEE 754 floating point
// decimal(P,S)	Fixed-point decimal; precision P, scale S	Scale is fixed [1], precision must be 38 or less
// date	Calendar date without timezone or time
// time	Time of day without date, timezone	Microsecond precision [2]
// timestamp	Timestamp without timezone	Microsecond precision [2]
// timestamptz	Timestamp with timezone	Stored as UTC [2]
// string	Arbitrary-length character sequences	Encoded with UTF-8 [3]
// uuid	Universally unique identifiers	Should use 16-byte fixed
// fixed(L)	Fixed-length byte array of length L
// binary

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { p: u8, s: u8 },
    Date,
    Time,
    Timestamp,
    Timestamptz,
    String,
    Uuid,
    Fixed(usize),
    Binary,
}

impl TryFrom<PrimitiveType> for DataType {
    type Error = IcebergError;

    fn try_from(value: PrimitiveType) -> Result<Self> {
        Ok(match value {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { p, s } => DataType::Decimal128(p, s as i8),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => unimplemented!(),
            PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::String => DataType::Utf8,
            _ => unimplemented!(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    format_version: i32,
    table_uuid: String,
    location: String,
    last_updated_ms: i64,
    last_column_id: i32,
    schemas: Vec<Schema>,
    current_schema_id: i32,
    // partition_specs: Vec<PartitionSpec>,
    default_spec_id: i32,
    last_partition_id: i32,
    properties: Option<HashMap<String, String>>,
    current_snapshot_id: Option<i64>,
    snapshots: Vec<Snapshot>,
    snapshot_log: Vec<SnapshotLog>,
    metadata_log: Vec<MetadataLog>,
    // sort_orders: Vec<SortOrder>,
    default_sort_order_id: i32,
    // refs: Option<HashMap<String, SnapshotReference>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    schema_id: i32,
    identifier_field_ids: Option<Vec<i32>>,
    fields: Vec<Field>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Field {
    id: i32,
    name: String,
    required: bool,
    r#type: String, // TODO
    doc: Option<String>,
    initial_value: Option<String>, // TODO
    write_default: Option<String>, // TODO
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    snapshot_id: i64,
    timestamp_ms: i64,
    summary: HashMap<String, String>,
    manifest_list: String,
    schema_id: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLog {
    snapshot_id: i64,
    timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLog {
    metadata_file: String,
    timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Reference {
    snapshot_id: i64,
    r#type: String,
}
