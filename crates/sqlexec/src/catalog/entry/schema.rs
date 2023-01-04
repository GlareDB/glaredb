use crate::catalog::constants::{
    DEFAULT_SCHEMA, INFORMATION_SCHEMA, INTERNAL_SCHEMA, POSTGRES_SCHEMA,
};
use serde::{Deserialize, Serialize};

struct DefaultSchema {
    name: &'static str,
}

const DEFAULT_SCHEMAS: &[DefaultSchema] = &[
    DefaultSchema {
        name: INFORMATION_SCHEMA,
    },
    DefaultSchema {
        name: INTERNAL_SCHEMA,
    },
    DefaultSchema {
        name: POSTGRES_SCHEMA,
    },
    DefaultSchema {
        name: DEFAULT_SCHEMA,
    },
];

#[derive(Debug, Clone, Serialize, Deserialize)]
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
