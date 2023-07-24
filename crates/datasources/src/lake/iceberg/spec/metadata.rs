use super::Schema;

use crate::lake::iceberg::errors::{IcebergError, Result};
use once_cell::sync::Lazy;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// On disk table metadata.
///
/// JSON serialization only.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub schemas: Vec<Schema>,
    pub current_schema_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub last_partition_id: i32,
    pub properties: Option<HashMap<String, String>>,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<Snapshot>,
    pub snapshot_log: Vec<SnapshotLog>,
    pub metadata_log: Vec<MetadataLog>,
    // sort_orders: Vec<SortOrder>,
    pub default_sort_order_id: i32,
    // refs: Option<HashMap<String, SnapshotReference>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    pub summary: HashMap<String, String>,
    pub manifest_list: String,
    pub schema_id: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLog {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLog {
    pub metadata_file: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: String, // TODO
}
