//! List types for GCS JSON apis.

use serde::{Deserialize, Serialize};

// <https://cloud.google.com/storage/docs/json_api/v1/objects/list>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsListResponse {
    /// Always "storage#objects".
    pub kind: String,
    pub next_page_token: Option<String>,
    pub prefixes: Option<Vec<String>>,
    pub items: Option<Vec<GcsObjectResource>>,
}

// TODO: More fields, see <https://cloud.google.com/storage/docs/json_api/v1/objects#resource>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsObjectResource {
    pub name: String,
}
