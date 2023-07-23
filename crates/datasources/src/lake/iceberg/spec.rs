use crate::lake::iceberg::errors::{IcebergError, Result};
use apache_avro::{from_value, Reader};
use datafusion::arrow::{
    array::{Array, ArrayAccessor, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, Bytes};
use std::collections::HashMap;
use std::str::FromStr;

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

/// Primitive types supported in iceberg tables.
// TODO: struct, list, map
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl FromStr for PrimitiveType {
    type Err = IcebergError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "boolean" => PrimitiveType::Boolean,
            "int" => PrimitiveType::Int,
            "long" => PrimitiveType::Long,
            "float" => PrimitiveType::Float,
            "double" => PrimitiveType::Double,
            "date" => PrimitiveType::Date,
            "time" => PrimitiveType::Time,
            "timestamp" => PrimitiveType::Timestamp,
            "timestamptz" => PrimitiveType::Timestamptz,
            "string" => PrimitiveType::String,
            "uuid" => PrimitiveType::Uuid,
            other if other.starts_with("decimal") => {
                // Regex that matches:
                // decimal(15, 2)
                // decimal(15,2)
                static DECIMAL_RE: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r#"^decimal\((?P<p>\d+),\s?(?P<s>\d+)\)$"#).unwrap());

                let captures = DECIMAL_RE.captures(other).ok_or_else(|| {
                    IcebergError::DataInvalid(format!("Invalid decimal type: {other}"))
                })?;

                let p: u8 = captures
                    .name("p")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid decimal type: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| {
                        IcebergError::DataInvalid(format!("Decimal precision not a u8: {e}"))
                    })?;

                let s: u8 = captures
                    .name("s")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid decimal type: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| {
                        IcebergError::DataInvalid(format!("Decimal scale not a u8: {e}"))
                    })?;

                PrimitiveType::Decimal { p, s }
            }
            other => return Err(IcebergError::DataInvalid(format!("Invalid type: {other}"))),
        })
    }
}

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
    // partition_specs: Vec<PartitionSpec>,
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

/// On disk schema from table metadata.
///
/// JSON serialization only.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    pub schema_id: i32,
    pub identifier_field_ids: Option<Vec<i32>>,
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn to_arrow_schema(&self) -> Result<ArrowSchema> {
        let fields = self
            .fields
            .iter()
            .map(|f| f.to_arrow_field())
            .collect::<Result<Vec<_>>>()?;
        Ok(ArrowSchema::new(fields))
    }
}

/// Fields in a schema.
///
/// JSON serialization only.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(deserialize_with = "Field::deserialize_type")]
    pub r#type: PrimitiveType,
    pub doc: Option<String>,
    pub initial_value: Option<String>, // TODO
    pub write_default: Option<String>, // TODO
}

impl Field {
    pub fn to_arrow_field(&self) -> Result<ArrowField> {
        // TODO: This will need to be extended to support nested types (maps,
        // lists, structs).
        Ok(ArrowField::new(
            &self.name,
            self.r#type.try_into()?,
            !self.required,
        ))
    }

    /// Deserialize a string into a `PrimitiveType`.
    fn deserialize_type<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        PrimitiveType::from_str(s).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct StructField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    pub field_type: String, // TODO
    pub doc: Option<String>,
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
pub struct Reference {
    pub snapshot_id: i64,
    pub r#type: String,
}

#[derive(Debug, Clone)]
pub struct ManifestListEntry {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: i32, // 0: data, 1: deletes
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: i32,
    pub existing_files_count: i32,
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    pub partitions: Vec<FieldSummary>,
    pub key_metadata: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ManifestList {
    pub entries: Vec<ManifestListEntry>,
}

/// Utility macro for getting a value at some column/row in a record batch.
macro_rules! get_value {
    ($array_type:ty, $batch:expr, $col:expr, $row:expr) => {{
        let col: usize = $col;
        let row: usize = $row;
        let batch: &RecordBatch = $batch;
        batch
            .column(col)
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| {
                IcebergError::DataInvalid(format!(
                    "Invalid column value for column {col}, row {row}"
                ))
            })?
            .value(row)
    }};
}

impl ManifestList {
    /// Try to convert a record batch to a manifest list.
    pub fn try_from_batch(batch: RecordBatch) -> Result<ManifestList> {
        let mut entries = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            let ent = ManifestListEntry {
                manifest_path: get_value!(StringArray, &batch, 0, row).to_string(),
                manifest_length: get_value!(Int64Array, &batch, 1, row),
                partition_spec_id: get_value!(Int32Array, &batch, 2, row),
                content: get_value!(Int32Array, &batch, 3, row),
                sequence_number: get_value!(Int64Array, &batch, 4, row),
                min_sequence_number: get_value!(Int64Array, &batch, 5, row),
                added_snapshot_id: get_value!(Int64Array, &batch, 6, row),
                added_files_count: get_value!(Int32Array, &batch, 7, row),
                existing_files_count: get_value!(Int32Array, &batch, 8, row),
                deleted_files_count: get_value!(Int32Array, &batch, 9, row),
                added_rows_count: get_value!(Int64Array, &batch, 10, row),
                existing_rows_count: get_value!(Int64Array, &batch, 11, row),
                deleted_rows_count: get_value!(Int64Array, &batch, 12, row),
                partitions: Vec::new(),   // TODO
                key_metadata: Vec::new(), // TODO
            };
            entries.push(ent);
        }

        Ok(ManifestList { entries })
    }
}

#[derive(Debug, Clone)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: bool,
    pub lower_bound: Vec<u8>,
    pub upper_count: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ManifestMetadata {
    pub schema: Schema,
    pub schema_id: i32,
    // pub partition_spec: (), // TODO
    pub partition_spec_id: i32,
    pub format_version: i32,
    pub content: String, // "data" or "delete"
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
        let partition_spec = (); // TODO
        let partition_spec_id = get_metadata_as_i32(&m, "partition-spec-id")?;
        let format_version = get_metadata_as_i32(&m, "format-version")?;
        let content = String::from_utf8_lossy(get_metadata_field(&m, "content")?).to_string();

        let metadata = ManifestMetadata {
            schema,
            schema_id,
            // partition_spec,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_value() -> Result<()> {
        use datafusion::arrow::{
            array::Int64Builder,
            datatypes::{DataType, Field, Schema},
        };
        use std::sync::Arc;

        let mut builder = Int64Builder::new();
        builder.append_value(1);

        let schema = Arc::new(Schema::new(vec![Field::new("num", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

        let v: i64 = get_value!(Int64Array, &batch, 0, 0);
        assert_eq!(1, v);
        Ok(())
    }

    #[test]
    fn test_deserialize_long_field() {
        let json = r#"
            {
              "id" : 1,
              "name" : "l_orderkey",
              "required" : false,
              "type" : "long"
            }"#;

        let deserialized: Field = serde_json::from_str(json).unwrap();
        let expected = Field {
            id: 1,
            name: "l_orderkey".to_string(),
            required: false,
            r#type: PrimitiveType::Long,
            doc: None,
            initial_value: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_decimal_field() {
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : "decimal(15, 2)"
            }"#;

        let deserialized: Field = serde_json::from_str(json).unwrap();
        let expected = Field {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: PrimitiveType::Decimal { p: 15, s: 2 },
            doc: None,
            initial_value: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_decimal_no_space_field() {
        // See lack of space after comma in decimal type.
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : "decimal(15,2)"
            }"#;

        let deserialized: Field = serde_json::from_str(json).unwrap();
        let expected = Field {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: PrimitiveType::Decimal { p: 15, s: 2 },
            doc: None,
            initial_value: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }
}
