use std::collections::HashMap;

use rayexec_error::{Result, ResultExt};
use serde::{Deserialize, Serialize};

use super::schema::StructType;

/// Action for modifying a table.
///
/// See <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions>
// TODO: domain metadata, sidecar, checkpoint metadata
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    #[serde(rename = "metaData")]
    ChangeMetadata(ActionChangeMetadata),

    // TODO: stats, tags, deletionVector, baseRowId, ...
    #[serde(rename = "add")]
    AddFile(ActionAddFile),

    // TODO: Other fields...
    #[serde(rename = "remove")]
    RemoveFile(ActionRemoveFile),

    #[serde(rename = "cdc")]
    AddCdcFile(ActionAddCdcFile),

    #[serde(rename = "txn")]
    Transaction(ActionTransaction),

    #[serde(rename = "protocol")]
    Protocol(ActionProtocol),

    /// Commit provenance.
    ///
    /// Contains arbitrary json.
    #[serde(rename = "commitInfo")]
    CommitInfo(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionChangeMetadata {
    pub id: String, // GUID
    pub name: Option<String>,
    pub description: Option<String>,
    pub format: FormatSpec,
    pub schema_string: String,
}

impl ActionChangeMetadata {
    /// Deserializes the table schema from the metadata.
    ///
    /// The metadata stores this as a json string, so this second
    /// deserialization is needed to get the actual schema.
    pub fn deserialize_schema(&self) -> Result<StructType> {
        serde_json::from_str(&self.schema_string).context("failed to deserialize schema string")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionAddFile {
    pub path: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub modification_time: u64,
    pub data_change: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionRemoveFile {
    pub path: String,
    pub deletion_timestamp: Option<u64>,
    pub data_change: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionAddCdcFile {
    pub path: String,
    pub partition_values: HashMap<String, String>,
    pub size: u64,
    pub data_change: bool,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionTransaction {
    pub app_id: String,
    pub version: u64,
    pub last_updated: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionProtocol {
    pub min_reader_version: u32,
    pub min_writer_version: u32,
    pub reader_features: Option<Vec<String>>,
    pub writer_features: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormatSpec {
    // The file format, delta only supports parquet.
    pub provider: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_change_metadata() {
        let input = r#"
        {
          "metaData":{
            "id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
            "format":{"provider":"parquet","options":{}},
            "schemaString":"...",
            "partitionColumns":[],
            "configuration":{
              "appendOnly": "true"
            }
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let expected = Action::ChangeMetadata(ActionChangeMetadata {
            id: "af23c9d7-fff1-4a5a-a2c8-55c59bd782aa".to_string(),
            name: None,
            description: None,
            format: FormatSpec {
                provider: "parquet".to_string(),
            },
            schema_string: "...".to_string(),
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_add_partitioned() {
        let input = r#"
        {
          "add": {
            "path": "date=2017-12-10/part-000...c000.gz.parquet",
            "partitionValues": {"date": "2017-12-10"},
            "size": 841454,
            "modificationTime": 1512909768000,
            "dataChange": true,
            "baseRowId": 4071,
            "defaultRowCommitVersion": 41,
            "stats": "{\"numRecords\":1,\"minValues\":{\"val..."
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let partition_values = [("date".to_string(), "2017-12-10".to_string())]
            .into_iter()
            .collect();
        let expected = Action::AddFile(ActionAddFile {
            path: "date=2017-12-10/part-000...c000.gz.parquet".to_string(),
            partition_values,
            size: 841454,
            modification_time: 1512909768000,
            data_change: true,
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_add_clustered() {
        let input = r#"
        {
          "add": {
            "path": "date=2017-12-10/part-000...c000.gz.parquet",
            "partitionValues": {},
            "size": 841454,
            "modificationTime": 1512909768000,
            "dataChange": true,
            "baseRowId": 4071,
            "defaultRowCommitVersion": 41,
            "clusteringProvider": "liquid",
            "stats": "{\"numRecords\":1,\"minValues\":{\"val..."
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let expected = Action::AddFile(ActionAddFile {
            path: "date=2017-12-10/part-000...c000.gz.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 841454,
            modification_time: 1512909768000,
            data_change: true,
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_remove() {
        let input = r#"
        {
          "remove": {
            "path": "part-00001-9…..snappy.parquet",
            "deletionTimestamp": 1515488792485,
            "baseRowId": 4071,
            "defaultRowCommitVersion": 41,
            "dataChange": true
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let expected = Action::RemoveFile(ActionRemoveFile {
            path: "part-00001-9…..snappy.parquet".to_string(),
            data_change: true,
            deletion_timestamp: Some(1515488792485),
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_cdc() {
        let input = r#"
        {
          "cdc": {
            "path": "_change_data/cdc-00001-c…..snappy.parquet",
            "partitionValues": {},
            "size": 1213,
            "dataChange": false
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let expected = Action::AddCdcFile(ActionAddCdcFile {
            path: "_change_data/cdc-00001-c…..snappy.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1213,
            data_change: false,
            tags: None,
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_txn() {
        let input = r#"
        {
          "txn": {
            "appId":"3ba13872-2d47-4e17-86a0-21afd2a22395",
            "version":364475
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let expected = Action::Transaction(ActionTransaction {
            app_id: "3ba13872-2d47-4e17-86a0-21afd2a22395".to_string(),
            version: 364475,
            last_updated: None,
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_protocol() {
        let input = r#"
        {
          "protocol":{
            "minReaderVersion":1,
            "minWriterVersion":2
          }
        }
        "#;

        let action: Action = serde_json::from_str(input).unwrap();
        let expected = Action::Protocol(ActionProtocol {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        });

        assert_eq!(expected, action);
    }

    #[test]
    fn action_commit_info() {
        // Abitrary commit info (from delta-rs). Not currently asserting the
        // parsed values, I just want to make sure it parses without error.
        let input = r#"
        {
           "commitInfo": {
             "timestamp":1689710090691,
             "operation":"WRITE",
             "operationParameters":{
               "mode":"Append"
             },
             "clientVersion":"delta-rs.0.13.0"
           }
         }"#;

        let action: Action = serde_json::from_str(input).unwrap();
        assert!(matches!(action, Action::CommitInfo(_)))
    }
}
