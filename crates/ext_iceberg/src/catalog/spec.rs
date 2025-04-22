use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
