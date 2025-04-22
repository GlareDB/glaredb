use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ErrorModel {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: i32,
}

#[derive(Debug, Serialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct CreateNamespaceResponse {
    pub naemspace: Vec<String>,
    pub properties: HashMap<String, String>,
}
