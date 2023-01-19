//! Builtins as determined by Metastore.
//!
//! On catalog initialization, whether that loading in a catalog from storage,
//! or creating a new one, as set of builtins will be inserted into the catalog.
//!
//! Two main takeaways:
//!
//! - Builtins are not persisted.
//! - Changing builtins just requires redeploying Metastore.
//!
//! However, there is one drawback to Metastore being the source-of-truth for
//! builtins. If we add or change a builtin table and redeploy Metastore, a
//! database node will be able to see it, but will not be able to execute
//! appropriately. We can revisit this if this isn't acceptable long-term.

use crate::types::catalog::ColumnDefinition;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use once_cell::sync::Lazy;
use pgrepr::oid::FIRST_GLAREDB_BUILTIN_ID;

/// Each GlareDB database only has a single catalog.
pub const DEFAULT_CATALOG: &str = "default";

/// Default schema that's created on every startup.
pub const DEFAULT_SCHEMA: &str = "public";

/// Internal schema for system tables.
pub const INTERNAL_SCHEMA: &str = "glare_catalog";

pub const INFORMATION_SCHEMA: &str = "information_schema";
pub const POSTGRES_SCHEMA: &str = "pg_catalog";

/// First oid available for other builtin objects.
///
/// All builtin schemas have a stable oid since all objects, including user
/// objects, rely on the oid of schemas.
pub const FIRST_NON_SCHEMA_ID: u32 = FIRST_GLAREDB_BUILTIN_ID + 100;

/// A builtin table.
#[derive(Debug, Clone)]
pub struct BuiltinTable {
    pub schema: &'static str,
    pub name: &'static str,
    pub columns: Vec<ColumnDefinition>,
}

pub static GLARE_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "views",
    columns: ColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("builtin", DataType::Boolean, false),
        ("schema_name", DataType::Utf8, false),
        ("view_name", DataType::Utf8, false),
        ("sql", DataType::Utf8, false),
    ]),
});

pub static GLARE_SCHEMAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "schemas",
    columns: ColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("builtin", DataType::Boolean, false),
        ("schema_name", DataType::Utf8, false),
    ]),
});

pub static GLARE_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "tables",
    columns: ColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("builtin", DataType::Boolean, false),
        ("schema_name", DataType::Utf8, false),
        ("table_name", DataType::Utf8, false),
    ]),
});

pub static GLARE_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "columns",
    columns: ColumnDefinition::from_tuples([
        ("table_oid", DataType::UInt32, false),
        ("schema_name", DataType::Utf8, false),
        ("table_name", DataType::Utf8, false),
        ("column_name", DataType::Utf8, false),
        ("column_index", DataType::UInt32, false),
        ("data_type", DataType::Utf8, false),
        ("is_nullable", DataType::Boolean, false),
    ]),
});

pub static GLARE_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "connections",
    columns: ColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("builtin", DataType::Boolean, false),
        ("schema_name", DataType::Utf8, false),
        ("connection_name", DataType::Utf8, false),
        ("connection_type", DataType::Utf8, false),
    ]),
});

impl BuiltinTable {
    /// Check if this table matches the provided schema and name.
    pub fn matches(&self, schema: &str, name: &str) -> bool {
        self.schema == schema && self.name == name
    }

    /// Get the arrow schema for the builtin table.
    pub fn arrow_schema(&self) -> ArrowSchema {
        ArrowSchema::new(
            self.columns
                .iter()
                .map(|col| ArrowField::new(&col.name, col.arrow_type.clone(), col.nullable))
                .collect(),
        )
    }

    /// Return a vector of all builtin tables.
    pub fn builtins() -> Vec<&'static BuiltinTable> {
        vec![
            &GLARE_VIEWS,
            &GLARE_SCHEMAS,
            &GLARE_TABLES,
            &GLARE_COLUMNS,
            &GLARE_CONNECTIONS,
        ]
    }
}

/// A builtin schema.
#[derive(Debug, Clone)]
pub struct BuiltinSchema {
    pub name: &'static str,
    pub oid: u32,
}

pub static SCHEMA_INTERNAL: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INTERNAL_SCHEMA,
    oid: 16384,
});

pub static SCHEMA_DEFAULT: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: DEFAULT_SCHEMA,
    oid: 16385,
});

pub static SCHEMA_INFORMATION: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INFORMATION_SCHEMA,
    oid: 16386,
});

pub static SCHEMA_POSTGRES: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: POSTGRES_SCHEMA,
    oid: 16387,
});

impl BuiltinSchema {
    pub fn builtins() -> Vec<&'static BuiltinSchema> {
        vec![
            &SCHEMA_INTERNAL,
            &SCHEMA_DEFAULT,
            &SCHEMA_INFORMATION,
            &SCHEMA_POSTGRES,
        ]
    }
}

/// A builtin view.
#[derive(Debug, Clone)]
pub struct BuiltinView {
    pub schema: &'static str,
    pub name: &'static str,
    pub sql: &'static str,
}

pub static INFORMATION_SCHEMA_SCHEMATA: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "schemata",
    sql: "
SELECT
    null AS catalog_name,
    null AS schema_name,
    null AS schema_owner",
});

pub static INFORMATION_SCHEMA_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "tables",
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
});

pub static INFORMATION_SCHEMA_COLUMNS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "columns",
    sql: "
SELECT
    null AS table_catalog,
    null AS table_schema,
    null AS table_name,
    null AS column_name,
    null AS ordinal_position,
    null AS column_default,
    null AS is_nullable,
    null AS data_type",
});

pub static PG_AM: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_am",
    sql: "
SELECT
    null AS oid,
    null AS amname,
    null AS amhandler,
    null AS amtype",
});

impl BuiltinView {
    pub fn builtins() -> Vec<&'static BuiltinView> {
        vec![
            &INFORMATION_SCHEMA_SCHEMATA,
            &INFORMATION_SCHEMA_TABLES,
            &INFORMATION_SCHEMA_COLUMNS,
            &PG_AM,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_schema_oid_range() {
        for schema in BuiltinSchema::builtins() {
            assert!(schema.oid < FIRST_NON_SCHEMA_ID);
            assert!(schema.oid >= FIRST_GLAREDB_BUILTIN_ID);
        }
    }
}
