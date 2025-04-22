use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ErrorModel {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: i32,
}


#[derive(Debug, Serialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct CreateNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct GetNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateNamespacePropertiesRequest {
    pub removals: Vec<String>,
    pub updates: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNamespacePropertiesResponse {
    pub removed: Vec<String>,
    pub updated: Vec<String>,
    pub missing: Vec<String>,
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub fields: Vec<SchemaField>,
    pub schema_id: Option<i32>,
    pub identifier_field_ids: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: bool,
    pub fields: Option<Vec<SchemaField>>,
    pub element_id: Option<i32>,
    pub element_required: Option<bool>,
    pub key_id: Option<i32>,
    pub key_required: Option<bool>,
    pub value_id: Option<i32>,
    pub value_required: Option<bool>,
    pub initial_default: Option<serde_json::Value>,
    pub write_default: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortField {
    pub source_id: i32,
    pub transform: String,
    pub direction: String,
    pub null_order: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_updated_ms: i64,
    pub properties: HashMap<String, String>,
    pub schemas: Vec<Schema>,
    pub current_schema_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub last_partition_id: i32,
    pub sort_orders: Vec<SortOrder>,
    pub default_sort_order_id: i32,
    pub last_sort_order_id: i32,
    pub snapshot_log: Vec<SnapshotLogEntry>,
    pub metadata_log: Vec<MetadataLogEntry>,
    pub last_sequence_number: i64,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Option<Vec<Snapshot>>,
    pub snapshot_id: Option<i64>,
    pub refs: Option<HashMap<String, SnapshotReference>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortOrder {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    pub metadata_file: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
    pub schema_id: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotReference {
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub reference_type: String,
    pub max_ref_age_ms: Option<i64>,
    pub max_snapshot_age_ms: Option<i64>,
    pub min_snapshots_to_keep: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<PartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableResponse {
    pub metadata: TableMetadata,
    pub metadata_location: String,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct LoadTableResponse {
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub config: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct UpdateTableRequest {
    pub requirements: Option<Vec<TableRequirement>>,
    pub updates: Option<Vec<TableUpdate>>,
}

#[derive(Debug, Serialize)]
pub struct TableRequirement {
    #[serde(rename = "type")]
    pub requirement_type: String,
    pub r_snapshot_id: Option<i64>,
    pub r_last_assigned_field_id: Option<i32>,
    pub r_current_schema_id: Option<i32>,
    pub r_last_assigned_partition_id: Option<i32>,
    pub r_default_spec_id: Option<i32>,
    pub r_last_assigned_sort_order_id: Option<i32>,
    pub r_default_sort_order_id: Option<i32>,
    pub r_uuid: Option<String>,
    pub r_branch: Option<String>,
    pub r_branch_snapshot_id: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct TableUpdate {
    #[serde(rename = "type")]
    pub update_type: String,
    pub action: Option<String>,
    pub snapshot_id: Option<i64>,
    pub schema: Option<Schema>,
    pub transforms: Option<Vec<String>>,
    pub additions: Option<Vec<PartitionField>>,
    pub removals: Option<Vec<i32>>,
    pub order_id: Option<i32>,
    pub fields: Option<Vec<SortField>>,
    pub properties: Option<HashMap<String, String>>,
    pub removals_list: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTableResponse {
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub config: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct RenameTableRequest {
    pub source: TableIdentifier,
    pub destination: TableIdentifier,
}

#[derive(Debug, Deserialize)]
pub struct RenameTableResponse {
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub config: Option<HashMap<String, String>>,
}
