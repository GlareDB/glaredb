use super::Schema;

use crate::lake::iceberg::errors::{IcebergError, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, str::FromStr};

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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    pub summary: HashMap<String, String>,
    pub manifest_list: String,
    pub schema_id: i32,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: PartitionTransform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// TODO: Spec also has "Partition Field" under json serialization section, not
// sure what that means.
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Void,
    Bucket(usize),
    Truncate(usize),
}

impl FromStr for PartitionTransform {
    type Err = IcebergError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "identity" => PartitionTransform::Identity,
            "year" => PartitionTransform::Year,
            "month" => PartitionTransform::Month,
            "day" => PartitionTransform::Day,
            "hour" => PartitionTransform::Hour,
            "void" => PartitionTransform::Void,

            // bucket
            other if other.starts_with("bucket") => {
                // Regex that matches:
                // bucket[16]
                static BUCKET_RE: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r#"^bucket\[(?P<n>\d+)\]$"#).unwrap());

                let captures = BUCKET_RE.captures(other).ok_or_else(|| {
                    IcebergError::DataInvalid(format!("Invalid bucket transform: {other}"))
                })?;

                let n: usize = captures
                    .name("n")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid bucket transform: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| IcebergError::DataInvalid(format!("'n' not a usize: {e}")))?;

                PartitionTransform::Bucket(n)
            }

            // truncate
            other if other.starts_with("truncate") => {
                // Regex that matches:
                // bucket[16]
                static BUCKET_RE: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r#"^truncate\[(?P<n>\d+)\]$"#).unwrap());

                let captures = BUCKET_RE.captures(other).ok_or_else(|| {
                    IcebergError::DataInvalid(format!("Invalid truncate transform: {other}"))
                })?;

                let n: usize = captures
                    .name("n")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid truncate transform: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| IcebergError::DataInvalid(format!("'n' not a usize: {e}")))?;

                PartitionTransform::Truncate(n)
            }

            other => {
                return Err(IcebergError::DataInvalid(format!(
                    "Invalid partition transform: {other}"
                )))
            }
        })
    }
}

impl<'de> Deserialize<'de> for PartitionTransform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        PartitionTransform::from_str(s).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_partition_transform() {
        // (json, expected)
        let test_cases = vec![
            ("identity", PartitionTransform::Identity),
            ("year", PartitionTransform::Year),
            ("month", PartitionTransform::Month),
            ("day", PartitionTransform::Day),
            ("hour", PartitionTransform::Hour),
            ("void", PartitionTransform::Void),
            ("bucket[16]", PartitionTransform::Bucket(16)),
            ("truncate[32]", PartitionTransform::Truncate(32)),
        ];

        for t in test_cases {
            let out: PartitionTransform = t.0.parse().unwrap();
            assert_eq!(t.1, out);
        }
    }
}
