use super::{PartitionField, PartitionSpec, Schema};

use crate::lake::iceberg::errors::{IcebergError, Result};
use apache_avro::{from_value, Reader};
use datafusion::arrow::{
    array::{Array, Int32Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, Bytes};
use std::fmt;
use std::{collections::HashMap, str::FromStr};

/// Manifest lists include summary medata for the table alongside the path the
/// actual manifest.
#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestListEntry {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: i32, // 0: data, 1: deletes
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    /// > Number of entries in the manifest that have status ADDED (1), when
    /// > null this is assumed to be non-zero
    pub added_files_count: Option<i32>,
    /// > Number of entries in the manifest that have status EXISTING (0), when
    /// > null this is assumed to be non-zero
    pub existing_files_count: Option<i32>,
    /// > Number of entries in the manifest that have status DELETED (2), when
    /// > null this is assumed to be non-zero
    pub deleted_files_count: Option<i32>,
    /// > Number of rows in all of files in the manifest that have status ADDED,
    /// > when null this is assumed to be non-zero
    pub added_rows_count: Option<i32>,
    pub existing_rows_count: Option<i32>,
    pub deleted_rows_count: i64,
    pub partitions: Vec<FieldSummary>,
    #[serde_as(as = "Option<Bytes>")]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ManifestList {
    pub entries: Vec<ManifestListEntry>,
}

impl ManifestList {
    /// Read a manifest list from a reader over an Avro file.
    pub fn from_raw_avro(reader: impl std::io::Read) -> Result<ManifestList> {
        let reader = Reader::new(reader)?;

        let mut entries = Vec::new();
        for value in reader {
            let value = value?;
            let entry: ManifestListEntry = from_value(&value)?;
            entries.push(entry);
        }

        Ok(ManifestList { entries })
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: bool,
    #[serde_as(as = "Option<Bytes>")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde_as(as = "Option<Bytes>")]
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
    type Err = IcebergError;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "data" => ManifestContent::Data,
            "delete" => ManifestContent::Delete,
            other => {
                return Err(IcebergError::DataInvalid(format!(
                    "'{other}' is not valid content for manifest"
                )))
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
        let reader = Reader::new(reader)?;

        let m = reader.user_metadata();

        fn get_metadata_field<'a, 'b>(
            m: &'a HashMap<String, Vec<u8>>,
            field: &'b str,
        ) -> Result<&'a Vec<u8>> {
            m.get(field).ok_or_else(|| {
                IcebergError::DataInvalid(format!("Missing field '{field}' in manifest metadata"))
            })
        }

        fn get_metadata_as_i32(m: &HashMap<String, Vec<u8>>, field: &str) -> Result<i32> {
            let bs = get_metadata_field(m, field)?;
            String::from_utf8_lossy(bs).parse::<i32>().map_err(|e| {
                IcebergError::DataInvalid(format!("Failed to parse 'schema-id' as an i32: {e}"))
            })
        }

        let schema = serde_json::from_slice::<Schema>(get_metadata_field(&m, "schema")?)?;
        // Spec says schema id is required, but seems like it's actually
        // optional. Missing from the spark outputs.
        let schema_id = get_metadata_as_i32(&m, "schema-id").unwrap_or_default();
        let partition_spec = serde_json::from_slice(m.get("partition-spec").ok_or_else(|| {
            IcebergError::DataInvalid(format!(
                "Missing field 'partition-spec' in manifest metadata"
            ))
        })?)?;
        let partition_spec_id = get_metadata_as_i32(&m, "partition-spec-id")?;
        let format_version = get_metadata_as_i32(&m, "format-version")?;
        let content = String::from_utf8_lossy(get_metadata_field(&m, "content")?).parse()?;

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
            let value = value?;
            let entry: ManifestEntry = from_value(&value)?;
            entries.push(entry);
        }

        Ok(Manifest { metadata, entries })
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

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
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
    #[serde_as(as = "Option<Bytes>")]
    pub key_metadata: Option<Vec<u8>>,
    pub split_offsets: Option<Vec<i64>>,
    pub equality_ids: Option<Vec<i32>>,
    pub sort_order_id: Option<i32>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryEntry {
    key: i32,
    #[serde_as(as = "Bytes")]
    value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct I64Entry {
    key: i32,
    value: i64,
}
