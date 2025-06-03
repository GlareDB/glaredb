use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use apache_avro::{Reader, from_value};
use glaredb_error::{DbError, Result, ResultExt};
use serde::{Deserialize, Serialize};

use super::{PartitionField, Schema};
use crate::table::spec::PartitionSpec;

/// An entry in the manifest list.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestFile {
    /// v1: required
    /// v2: required
    /// v3: required
    pub manifest_path: String,
    /// v1: required
    /// v2: required
    /// v3: required
    pub manifest_length: i64,
    /// v1: required
    /// v2: required
    /// v3: required
    pub partition_spec_id: i32,
    /// > The type of files tracked by the manifest, either data or delete
    /// > files; 0 for all v1 manifests
    ///
    /// `0`: data
    /// `1`: deletes
    ///
    /// v1: n/a
    /// v2: required
    /// v3: required
    #[serde(default)] // v1 is always "data" (no deletes)
    pub content: i32, // Convert to ManifestContent
    /// v1: n/a
    /// v2: required
    /// v3: required
    #[serde(default)]
    pub sequence_number: i64,
    /// v1: n/a
    /// v2: required
    /// v3: required
    #[serde(default)]
    pub min_sequence_number: i64,
    /// v1: required
    /// v2: required
    /// v3: required
    pub added_snapshot_id: i64,
    /// > Number of entries in the manifest that have status ADDED (1), when
    /// > null this is assumed to be non-zero
    ///
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Avro is terrible format.
    pub added_files_count: i32,
    /// > Number of entries in the manifest that have status EXISTING (0), when
    /// > null this is assumed to be non-zero
    ///
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Avro is terrible format.
    pub existing_files_count: i32,
    /// > Number of entries in the manifest that have status DELETED (2), when
    /// > null this is assumed to be non-zero
    ///
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Avro is terrible format.
    pub deleted_files_count: i32,
    /// > Number of rows in all of files in the manifest that have status ADDED,
    /// > when null this is assumed to be non-zero
    ///
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Avro is terrible format.
    pub added_rows_count: i64,
    /// > Number of rows in all of files in the manifest that have status
    /// > EXISTING, when null this is assumed to be non-zero
    ///
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Avro is terrible format.
    pub existing_rows_count: i64,
    /// v1: optional
    /// v2: required
    /// v3: required
    #[serde(default)] // Avro is terrible format.
    pub deleted_rows_count: i64,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    #[serde(default)]
    pub partitions: Vec<FieldSummary>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    #[serde(with = "serde_bytes")]
    #[serde(default)]
    pub key_metadata: Option<Vec<u8>>,
    /// v1: n/a
    /// v2: n/a
    /// v3: optional
    #[serde(default)] // Avro is terrible format.
    pub first_row_id: i64,
}

/// A list of manifest files.
#[derive(Debug, Clone)]
pub struct ManifestList {
    pub entries: Vec<ManifestFile>,
}

impl ManifestList {
    /// Read a manifest list from serialized bytes.
    pub fn from_raw_avro(bs: &[u8]) -> Result<ManifestList> {
        let reader = Reader::new(bs).context("Failed to create avro reader for manifest list")?;

        let mut entries = Vec::new();
        for value in reader {
            let value = value.context("Failed to get value for manifest list entry")?;
            let entry: ManifestFile = from_value(&value)
                .context("Failed to deserialize value for manifest list entry")?;

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

// this is some insane shit
#[derive(Debug, Clone)]
pub struct ManifestMetadata {
    /// v1: required
    /// v2: required
    pub schema: Schema,
    /// v1: optional
    /// v2: required
    pub schema_id: i32,
    /// v1: required
    /// v2: required
    pub partition_spec: Vec<PartitionField>,
    /// v1: optional
    /// v2: required
    pub partition_spec_id: i32,
    /// v1: optional
    /// v2: required
    pub format_version: i32,
    /// v1: n/a
    /// v2: required
    pub content: ManifestContent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ManifestContent {
    Data,
    Delete,
}

impl From<ManifestContent> for i32 {
    fn from(value: ManifestContent) -> Self {
        match value {
            ManifestContent::Data => 0,
            ManifestContent::Delete => 1,
        }
    }
}

impl TryFrom<i32> for ManifestContent {
    type Error = DbError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Data,
            1 => Self::Delete,
            i => {
                return Err(DbError::new(format!("unknown manifest content: {i}")));
            }
        })
    }
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

/// A manifest!
#[derive(Debug, Clone)]
pub struct Manifest {
    /// Parsed metadata from the manifest kv metadata.
    pub metadata: ManifestMetadata,
    /// Entries in the manifest file.
    pub entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub fn from_raw_avro(bs: &[u8]) -> Result<Manifest> {
        let reader = Reader::new(bs)
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
                .context_fn(|| format!("Failed to parse '{field}' as an i32"))
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
            let value = value.context("failed to get value for manifest entry")?;
            let entry: ManifestEntry =
                from_value(&value).context("failed to deserialize value for manifest entry")?;
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
    pub const fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted)
    }
}

impl TryFrom<i32> for ManifestEntryStatus {
    type Error = DbError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
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
    /// v1: required
    /// v2: required
    pub status: ManifestEntryStatus,
    /// v1: required
    /// v2: optional
    pub snapshot_id: Option<i64>,
    /// v1: n/a
    /// v2: optional
    pub sequence_number: Option<i64>,
    /// v1: n/a
    /// v2: optional
    pub file_sequence_number: Option<i64>,
    /// v1: required
    /// v2: required
    pub data_file: DataFile,
}

#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub enum DataFileContent {
    #[default]
    Data,
    PositionDeletes,
    EqualityDeletes,
}

impl TryFrom<i32> for DataFileContent {
    type Error = DbError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Data,
            1 => Self::PositionDeletes,
            2 => Self::EqualityDeletes,
            i => {
                return Err(DbError::new(format!("unknown data file content: {i}")));
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    /// v1: n/a
    /// v2: required
    /// v3: required
    #[serde(default)]
    pub content: DataFileContent,
    /// v1: required
    /// v2: required
    /// v3: required
    pub file_path: String,
    /// v1: required
    /// v2: required
    /// v3: required
    pub file_format: String,
    // TODO: partition
    /// v1: required
    /// v2: required
    /// v3: required
    pub record_count: i64,
    /// v1: required
    /// v2: required
    /// v3: required
    pub file_size_in_bytes: i64,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub column_sizes: Option<Vec<I64Entry>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub value_counts: Option<Vec<I64Entry>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub null_value_counts: Option<Vec<I64Entry>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub nan_value_counts: Option<Vec<I64Entry>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub lower_bounds: Option<Vec<BinaryEntry>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub upper_bounds: Option<Vec<BinaryEntry>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    #[serde(with = "serde_bytes")]
    pub key_metadata: Option<Vec<u8>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub split_offsets: Option<Vec<i64>>,
    /// v1: n/a
    /// v2: optional
    /// v3: optional
    pub equality_ids: Option<Vec<i32>>,
    /// v1: optional
    /// v2: optional
    /// v3: optional
    pub sort_order_id: Option<i32>,
    // TODO: The rest
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryEntry {
    pub key: i32,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct I64Entry {
    pub key: i32,
    pub value: i64,
}
