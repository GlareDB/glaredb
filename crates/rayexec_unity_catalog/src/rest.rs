use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::connection::ListResponseBody;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnityListSchemasResponse {
    pub schemas: Vec<UnitySchemaInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitySchemaInfo {
    pub name: String,
    pub catalog_name: String,
    pub comment: String,
    // pub properties: HashMap<String, String>,
    pub full_name: String,
    pub owner: String,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: i64,
    pub schema_id: String,
}

impl ListResponseBody for UnityListSchemasResponse {
    fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnityListTablesResponse {
    pub tables: Vec<UnityTableInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnityTableInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    /// MANAGED or EXTERNAL.
    pub table_type: String,
    /// DELTA, CSV, JSON, AVRO, PARQUET, ORC, or TEXT
    pub data_source_format: String,
    pub columns: Vec<UnityColumnInfo>,
    pub storage_location: String,
    pub comment: String,
    // pub properties: HashMap<String, String>,
    pub owner: String,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: i64,
    pub updated_by: String,
    pub table_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnityColumnInfo {
    pub name: String,
    pub type_text: String,
    pub type_json: String,
    /// BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP,
    /// TIMESTAMP_NTZ, STRING, BINARY, DECIMAL, INTERVAL, ARRAY, STRUCT, MAP,
    /// CHAR, NULL, USER_DEFINED_TYPE, or TABLE_TYPE
    pub type_name: String,
    pub type_precision: i32,
    pub type_scale: i32,
    pub type_interval_type: String,
    pub position: i32,
    pub comment: String,
    pub nullable: bool,
    pub partition_index: i32,
}

impl ListResponseBody for UnityListTablesResponse {
    fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}
