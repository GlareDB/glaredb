use serde::{Deserialize, Serialize};

struct DefaultSchema {
    name: &'static str,
}

const DEFAULT_SCHEMAS: &[DefaultSchema] = &[
    DefaultSchema {
        name: "information_schema",
    },
    DefaultSchema {
        name: "glare_catalog",
    },
    DefaultSchema { name: "pg_catalog" },
];

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub schema: String,
    pub internal: bool,
}

impl SchemaEntry {
    pub fn generate_defaults() -> impl Iterator<Item = SchemaEntry> {
        DEFAULT_SCHEMAS.iter().map(|def| SchemaEntry {
            schema: def.name.to_string(),
            internal: true,
        })
    }
}
