use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;

use glaredb_error::{DbError, Result};
use regex::Regex;
use serde::{Deserialize, Deserializer, de};

use super::Schema;

/// On disk table metadata.
///
/// JSON serialization only.
// TODO: Very big (and will get bigger), add some Cow.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Metadata {
    /// v1: required
    /// v2: required
    /// v3: required
    pub format_version: i32,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub table_uuid: Option<String>,
    /// v1: required
    /// v2: required
    /// v3: required
    pub location: String,
    /// v1: n/a
    /// v2: required
    /// v3: required
    pub last_sequence_number: Option<i64>,
    /// v1: required
    /// v2: required
    /// v3: required
    pub last_updated_ms: i64,
    /// v1: required
    /// v2: required
    /// v3: required
    pub last_column_id: i32,
    /// v1: required
    /// v2: n/a
    /// v3: n/a
    pub schema: Option<Schema>,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub schemas: Vec<Schema>,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub current_schema_id: Option<i32>,
    /// v1: required
    /// v2: n/a
    /// v3: n/a
    pub partition_spec: Option<PartitionSpec>,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub partition_specs: Vec<PartitionSpec>,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub default_spec_id: i32,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub last_partition_id: i32,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    #[serde(default)] // Empty map if not provided.
    pub properties: HashMap<String, String>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub current_snapshot_id: Option<i64>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub snapshots: Vec<Snapshot>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub snapshot_log: Vec<SnapshotLog>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub metadata_log: Vec<MetadataLog>,
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Empty vec if not provided.
    pub sort_orders: Vec<SortOrder>,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub default_sort_order_id: Option<i32>,
    /// v1: n/a
    /// v2: n/a
    /// v3: required
    pub next_row_id: Option<i64>,
    // TODO: Figure out what this field is for.
    // refs: Option<HashMap<String, SnapshotReference>>,
    // TODO: The rest
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    /// v1: required
    /// v2: required
    /// v3: required
    pub snapshot_id: i64,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub parent_snapshot_id: Option<i64>,
    /// v1: n/a
    /// v2: required
    /// v3: required
    pub sequence_number: Option<i64>,
    /// v1: required
    /// v2: required
    /// v3: required
    pub timestamp_ms: i64,
    /// v1: optional
    /// v2: required
    /// v3: required
    pub manifest_list: Option<String>,
    /// v1: optional
    /// v2: n/a
    /// v3: n/a
    #[serde(default)]
    pub manifests: Vec<String>,
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)]
    pub summary: HashMap<String, String>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub schema_id: i32,
    /// v1: n/a
    /// v2: n/a
    /// v3: required
    pub first_row_id: Option<i64>,
    // TODO: key-id (for encryption)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLog {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLog {
    pub metadata_file: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: Transform,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    pub transform: Transform,
    pub source_id: i32,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// TODO: Spec also has "Partition Field" under json serialization section, not
// sure what that means.
pub enum Transform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Void,
    Bucket(usize),
    Truncate(usize),
}

impl FromStr for Transform {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,

            // bucket
            other if other.starts_with("bucket") => {
                // Regex that matches:
                // bucket[16]
                static BUCKET_RE: LazyLock<Regex> =
                    LazyLock::new(|| Regex::new(r"^bucket\[(?P<n>\d+)\]$").unwrap());

                let captures = BUCKET_RE
                    .captures(other)
                    .ok_or_else(|| DbError::new(format!("Invalid bucket transform: {other}")))?;

                let n: usize = captures
                    .name("n")
                    .ok_or_else(|| DbError::new(format!("Invalid bucket transform: {other}")))?
                    .as_str()
                    .parse()
                    .map_err(|e| DbError::new(format!("'n' not a usize: {e}")))?;

                Transform::Bucket(n)
            }

            // truncate
            other if other.starts_with("truncate") => {
                // Regex that matches:
                // bucket[16]
                static BUCKET_RE: LazyLock<Regex> =
                    LazyLock::new(|| Regex::new(r"^truncate\[(?P<n>\d+)\]$").unwrap());

                let captures = BUCKET_RE
                    .captures(other)
                    .ok_or_else(|| DbError::new(format!("Invalid truncate transform: {other}")))?;

                let n: usize = captures
                    .name("n")
                    .ok_or_else(|| DbError::new(format!("Invalid truncate transform: {other}")))?
                    .as_str()
                    .parse()
                    .map_err(|e| DbError::new(format!("'n' not a usize: {e}")))?;

                Transform::Truncate(n)
            }

            other => return Err(DbError::new(format!("Invalid transform: {other}"))),
        })
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        Transform::from_str(s).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_transform() {
        // (json, expected)
        let test_cases = vec![
            ("identity", Transform::Identity),
            ("year", Transform::Year),
            ("month", Transform::Month),
            ("day", Transform::Day),
            ("hour", Transform::Hour),
            ("void", Transform::Void),
            ("bucket[16]", Transform::Bucket(16)),
            ("truncate[32]", Transform::Truncate(32)),
        ];

        for t in test_cases {
            let out: Transform = t.0.parse().unwrap();
            assert_eq!(t.1, out);
        }
    }

    #[test]
    fn test_deserialize_partiton_field() {
        let json = r#"
            {
              "source-id": 4,
              "field-id": 1000,
              "name": "ts_day",
              "transform": "day"
            }"#;

        let deserialized: PartitionField = serde_json::from_str(json).unwrap();
        let expected = PartitionField {
            source_id: 4,
            field_id: 1000,
            name: "ts_day".to_string(),
            transform: Transform::Day,
        };

        assert_eq!(expected, deserialized);
    }
}
