use crate::constants::INFORMATION_SCHEMA;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewEntry {
    pub schema: String,
    pub name: String,
    pub column_count: u32,
    pub sql: String,
}

struct DefaultViewEntry {
    schema: &'static str,
    name: &'static str,
    column_count: u32,
    sql: &'static str,
}

impl ViewEntry {
    pub fn generate_defaults() -> impl Iterator<Item = ViewEntry> {
        [DefaultViewEntry {
            schema: INFORMATION_SCHEMA,
            name: "tables",
            column_count: 4,
            sql: "
SELECT
    null AS table_catalog,
    schema_name AS table_schema,
    table_name,
    'BASE TABLE' AS table_type
FROM glare_catalog.tables
UNION ALL
SELECT null AS
    table_catalog,
    schema_name AS table_schema,
    view_name AS table_name,
    'VIEW' AS table_type
FROM glare_catalog.views",
        }]
        .map(|ent| ViewEntry {
            schema: ent.schema.to_string(),
            name: ent.name.to_string(),
            column_count: ent.column_count,
            sql: ent.sql.trim().to_string(),
        })
        .into_iter()
    }
}
