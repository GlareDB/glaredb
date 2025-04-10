use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use apache_avro::{Reader, from_value};
use glaredb_error::{DbError, Result, ResultExt};
use serde::{Deserialize, Serialize};

use super::{PartitionField, Schema};
use crate::spec::PartitionSpec;

/// Manifest lists include summary medata for the table alongside the path the
/// actual manifest.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestListEntry {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    /// > The type of files tracked by the manifest, either data or delete
    /// > files; 0 for all v1 manifests
    ///
    /// `0`: data
    /// `1`: deletes
    #[serde(default)]
    pub content: i32,
    #[serde(default)]
    pub sequence_number: i64,
    #[serde(default)]
    pub min_sequence_number: i64,
    #[serde(default)]
    pub added_snapshot_id: i64,
    /// > Number of entries in the manifest that have status ADDED (1), when
    /// > null this is assumed to be non-zero
    // TODO: Remove default and deserialize into something more meaningful.
    #[serde(default)]
    pub added_files_count: i32,
    /// > Number of entries in the manifest that have status EXISTING (0), when
    /// > null this is assumed to be non-zero
    #[serde(default)]
    pub existing_files_count: i32,
    /// > Number of entries in the manifest that have status DELETED (2), when
    /// > null this is assumed to be non-zero
    #[serde(default)]
    pub deleted_files_count: i32,
    /// > Number of rows in all of files in the manifest that have status ADDED,
    /// > when null this is assumed to be non-zero
    #[serde(default)]
    pub added_rows_count: i32,
    /// > Number of rows in all of files in the manifest that have status
    /// > EXISTING, when null this is assumed to be non-zero
    #[serde(default)]
    pub existing_rows_count: i32,
    pub deleted_rows_count: i64,
    pub partitions: Vec<FieldSummary>,
    #[serde(with = "serde_bytes")]
    #[serde(default)]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ManifestList {
    pub entries: Vec<ManifestListEntry>,
}

impl ManifestList {
    /// Read a manifest list from a reader over an Avro file.
    pub fn from_raw_avro(reader: impl std::io::Read) -> Result<ManifestList> {
        let reader = Reader::new(reader).map_err(|e| {
            DbError::new(format!(
                "failed to create avro reader for manifest list: {e}"
            ))
        })?;

        let mut entries = Vec::new();
        for value in reader {
            let value = value.map_err(|e| {
                DbError::new(format!("failed to get value for manifest list entry: {e}"))
            })?;
            let entry: ManifestListEntry = from_value(&value).map_err(|e| {
                DbError::new(format!(
                    "failed to deserialize value for manifest list entry: {e}"
                ))
            })?;

            entries.push(entry);
        }

        Ok(ManifestList { entries })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: bool,
    #[serde(with = "serde_bytes")]
    #[serde(default)]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    #[serde(default)]
    pub upper_bound: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ManifestMetadata {
    pub schema: Schema,
    pub schema_id: i32,
    pub partition_spec: Vec<PartitionField>,
    pub partition_spec_id: i32,
    pub format_version: i32,
    pub content: ManifestContent,
}

#[derive(Debug, Clone, Copy)]
pub enum ManifestContent {
    Data,
    Delete,
}

impl FromStr for ManifestContent {
    type Err = DbError;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "data" => ManifestContent::Data,
            "delete" => ManifestContent::Delete,
            other => {
                return Err(DbError::new(format!(
                    "'{other}' is not valid content for manifest"
                )));
            }
        })
    }
}

impl fmt::Display for ManifestContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestContent::Data => write!(f, "data"),
            ManifestContent::Delete => write!(f, "delete"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Manifest {
    pub metadata: ManifestMetadata,
    pub entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub fn from_raw_avro(reader: impl std::io::Read) -> Result<Manifest> {
        let reader = Reader::new(reader)
            .map_err(|e| DbError::new(format!("failed to create avro reader for manifest: {e}")))?;

        let m = reader.user_metadata();

        fn get_metadata_field<'a>(
            m: &'a HashMap<String, Vec<u8>>,
            field: &str,
        ) -> Result<&'a Vec<u8>> {
            m.get(field).ok_or_else(|| {
                DbError::new(format!("Missing field '{field}' in manifest metadata"))
            })
        }

        fn get_metadata_as_i32(m: &HashMap<String, Vec<u8>>, field: &str) -> Result<i32> {
            let bs = get_metadata_field(m, field)?;
            String::from_utf8_lossy(bs)
                .parse::<i32>()
                .map_err(|e| DbError::new(format!("Failed to parse 'schema-id' as an i32: {e}")))
        }

        let schema = serde_json::from_slice::<Schema>(get_metadata_field(m, "schema")?)
            .context("failed to deserialize schema json")?;
        // Spec says schema id is required, but seems like it's actually
        // optional. Missing from the spark outputs.
        let schema_id = get_metadata_as_i32(m, "schema-id").unwrap_or_default();

        let partition_spec_id = get_metadata_as_i32(m, "partition-spec-id")?;

        let raw_partition_spec = m.get("partition-spec").ok_or_else(|| {
            DbError::new("Missing field 'partition-spec' in manifest metadata".to_string())
        })?;

        let partition_spec = match serde_json::from_slice::<PartitionSpec>(raw_partition_spec) {
            Ok(spec) => spec.fields,
            Err(_e) => {
                // Try to get it as a slice of PartitionField.
                serde_json::from_slice(raw_partition_spec)
                    .context("failed to deserialize raw partition spec")?
            }
        };

        let format_version = get_metadata_as_i32(m, "format-version")?;
        let content = match get_metadata_field(m, "content") {
            Ok(c) => String::from_utf8_lossy(c).parse()?,
            Err(_) => ManifestContent::Data,
        };

        let metadata = ManifestMetadata {
            schema,
            schema_id,
            partition_spec,
            partition_spec_id,
            format_version,
            content,
        };

        let mut entries = Vec::new();
        for value in reader {
            let value = value.map_err(|e| {
                DbError::new(format!("failed to get value for manifest entry: {e}"))
            })?;
            let entry: ManifestEntry = from_value(&value).map_err(|e| {
                DbError::new(format!(
                    "failed to deserialize value for manifest entry: {e}"
                ))
            })?;
            entries.push(entry);
        }

        Ok(Manifest { metadata, entries })
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub enum ManifestEntryStatus {
    #[default]
    Existing,
    Added,
    Deleted,
}

impl ManifestEntryStatus {
    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted)
    }
}

impl TryFrom<i32> for ManifestEntryStatus {
    type Error = DbError;

    fn try_from(value: i32) -> std::prelude::v1::Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Existing,
            1 => Self::Added,
            2 => Self::Deleted,
            i => {
                return Err(DbError::new(format!("unknown manifest entry status: {i}")));
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub status: i32,
    /// Required in v2
    pub snapshot_id: Option<i64>,
    /// Required in v2
    pub sequence_number: Option<i64>,
    /// Required in v2
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    #[serde(default)]
    pub content: i32,
    pub file_path: String,
    pub file_format: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub column_sizes: Option<Vec<I64Entry>>,
    pub value_counts: Option<Vec<I64Entry>>,
    pub null_value_counts: Option<Vec<I64Entry>>,
    pub nan_value_counts: Option<Vec<I64Entry>>,
    pub distinct_counts: Option<Vec<I64Entry>>,
    pub lower_bounds: Option<Vec<BinaryEntry>>,
    pub upper_bounds: Option<Vec<BinaryEntry>>,
    #[serde(with = "serde_bytes")]
    #[serde(default)]
    pub key_metadata: Option<Vec<u8>>,
    pub split_offsets: Option<Vec<i64>>,
    pub equality_ids: Option<Vec<i32>>,
    pub sort_order_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryEntry {
    key: i32,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct I64Entry {
    key: i32,
    value: i64,
}
