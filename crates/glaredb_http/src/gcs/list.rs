//! List types for GCS JSON apis.

use serde::{Deserialize, Serialize};

// <https://cloud.google.com/storage/docs/json_api/v1/objects/list>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsListResponse<'a> {
    /// Always "storage#objects".
    pub kind: &'a str,
    pub next_page_token: Option<&'a str>,
    pub prefixes: Option<Vec<&'a str>>,
    pub items: Option<Vec<GcsObjectResource<'a>>>,
}

// TODO: More fields, see <https://cloud.google.com/storage/docs/json_api/v1/objects#resource>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsObjectResource<'a> {
    pub name: &'a str,
}
