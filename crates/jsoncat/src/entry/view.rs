use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewEntry {
    pub schema: String,
    pub name: String,
}
