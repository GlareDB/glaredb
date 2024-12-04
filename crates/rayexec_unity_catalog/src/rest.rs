use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::connection::ListResponseBody;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListUnitySchemasResponse {
    pub schemas: Vec<UnitySchemaInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitySchemaInfo {
    pub name: String,
    pub catalog_name: String,
    pub comment: String,
    pub properties: HashMap<String, String>,
    pub full_name: String,
    pub owner: String,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: i64,
    pub schema_id: String,
}

impl ListResponseBody for ListUnitySchemasResponse {
    fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}
