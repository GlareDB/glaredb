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

/// The default catalog that exists in all GlareDB databases.
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

#[derive(Debug, Clone)]
pub struct BuiltinDatabase {
    pub name: &'static str,
    pub oid: u32,
}

pub static DATABASE_DEFAULT: Lazy<BuiltinDatabase> = Lazy::new(|| BuiltinDatabase {
    name: DEFAULT_CATALOG,
    oid: FIRST_GLAREDB_BUILTIN_ID,
});

impl BuiltinDatabase {
    pub fn builtins() -> Vec<&'static BuiltinDatabase> {
        vec![&DATABASE_DEFAULT]
    }
}

/// A builtin table.
#[derive(Debug, Clone)]
pub struct BuiltinTable {
    pub schema: &'static str,
    pub name: &'static str,
    pub columns: Vec<ColumnDefinition>,
}

pub static GLARE_DATABASES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "databases",
    columns: ColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_name", DataType::Utf8, false),
        ("external", DataType::Boolean, false),
    ]),
});

pub static GLARE_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "views",
    columns: ColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("builtin", DataType::Boolean, false),
        ("schema_oid", DataType::UInt32, false),
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
        ("schema_oid", DataType::UInt32, false),
        ("schema_name", DataType::Utf8, false),
        ("table_name", DataType::Utf8, false),
        ("external", DataType::Boolean, false),
        ("connection_oid", DataType::UInt32, true),
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

pub static GLARE_EXTERNAL_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "external_columns",
    columns: ColumnDefinition::from_tuples([
        ("schema_oid", DataType::UInt32, false),
        ("table_oid", DataType::UInt32, false),
        ("column_name", DataType::Utf8, false),
        ("column_index", DataType::UInt32, false),
        ("data_type", DataType::Utf8, false),
        ("pg_data_type", DataType::Utf8, false), //TODO should this be Type OID
        ("is_nullable", DataType::Boolean, false),
    ]),
});

pub static GLARE_SESSION_QUERY_METRICS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "session_query_metrics",
    columns: ColumnDefinition::from_tuples([
        ("query_text", DataType::Utf8, false),
        ("result_type", DataType::Utf8, false),
        ("execution_status", DataType::Utf8, false),
        ("error_message", DataType::Utf8, true),
        ("elapsed_compute_ns", DataType::UInt64, true),
        ("output_rows", DataType::UInt64, true),
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
            &GLARE_EXTERNAL_COLUMNS,
            &GLARE_SESSION_QUERY_METRICS,
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
    oid: 16385,
});

pub static SCHEMA_DEFAULT: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: DEFAULT_SCHEMA,
    oid: 16386,
});

pub static SCHEMA_INFORMATION: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INFORMATION_SCHEMA,
    oid: 16387,
});

pub static SCHEMA_POSTGRES: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: POSTGRES_SCHEMA,
    oid: 16388,
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

// Information schema tables.
//
// See <https://www.postgresql.org/docs/current/information-schema.html>

pub static INFORMATION_SCHEMA_SCHEMATA: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "schemata",
    sql: "
SELECT
    'default' AS catalog_name,
    schema_name AS schema_name,
    null AS schema_owner,
    null AS default_character_set_catalog,
    null AS default_character_set_schema,
    null AS default_character_set_name,
    null AS sql_path
FROM glare_catalog.schemas",
});

pub static INFORMATION_SCHEMA_TABLES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "tables",
    sql: "
SELECT *
FROM (
    SELECT
        'default' AS table_catalog,
        schema_name AS table_schema,
        table_name AS table_name,
        'BASE TABLE' AS table_type,
        null AS self_referencing_column_name,
        null AS reference_generation,
        null AS user_defined_type_catalog,
        null AS user_defined_type_schema,
        null AS user_defined_type_name,
        'NO' AS is_insertable_into,
        'NO' AS is_typed,
        null AS commit_action
    FROM glare_catalog.tables
    UNION ALL
    SELECT
        'default' AS table_catalog,
        schema_name AS table_schema,
        view_name AS table_name,
        'VIEW' AS table_type,
        null AS self_referencing_column_name,
        null AS reference_generation,
        null AS user_defined_type_catalog,
        null AS user_defined_type_schema,
        null AS user_defined_type_name,
        'NO' AS is_insertable_into,
        'NO' AS is_typed,
        null AS commit_action
    FROM glare_catalog.views
)",
});

pub static INFORMATION_SCHEMA_COLUMNS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "columns",
    sql: "
SELECT
    'default' AS table_catalog,
    c.schema_name AS table_schema,
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.column_index + 1 AS ordinal_position,
    null AS column_default,
    c.is_nullable AS is_nullable,
    c.data_type AS data_type,
    null AS character_maximum_length,
    null AS numeric_precision,
    null AS numeric_precision_radix,
    null AS numeric_scale,
    null AS datetime_precision,
    null AS interval_type,
    null AS interval_precision,
    null AS character_set_catalog,
    null AS character_set_schema,
    null AS character_set_name,
    null AS collation_catalog,
    null AS collation_schema,
    null AS collation_name,
    null AS domain_catalog,
    null AS domain_schema,
    null AS domain_name,
    null AS udt_catalog,
    null AS udt_schema,
    null AS udt_name,
    null AS scope_catalog,
    null AS scope_schema,
    null AS scope_name,
    null AS maximum_cardinality,
    null AS dtd_identifier,
    null AS is_self_referencing,
    null AS is_identity,
    null AS identity_generation,
    null AS identity_start,
    null AS identity_increment,
    null AS identity_maximum,
    null AS identity_minimum,
    null AS identity_cyle,
    null AS is_generated,
    null AS generation_expression,
    'NO' AS is_updateable
FROM glare_catalog.columns c
UNION ALL
SELECT
    'default' AS table_catalog,
    s.schema_name AS table_schema,
    t.table_name AS table_name,
    c.column_name AS column_name,
    c.column_index + 1 AS ordinal_position,
    null AS column_default,
    c.is_nullable AS is_nullable,
    c.pg_data_type AS data_type,
    null AS character_maximum_length,
    null AS numeric_precision,
    null AS numeric_precision_radix,
    null AS numeric_scale,
    null AS datetime_precision,
    null AS interval_type,
    null AS interval_precision,
    null AS character_set_catalog,
    null AS character_set_schema,
    null AS character_set_name,
    null AS collation_catalog,
    null AS collation_schema,
    null AS collation_name,
    null AS domain_catalog,
    null AS domain_schema,
    null AS domain_name,
    null AS udt_catalog,
    null AS udt_schema,
    null AS udt_name,
    null AS scope_catalog,
    null AS scope_schema,
    null AS scope_name,
    null AS maximum_cardinality,
    null AS dtd_identifier,
    null AS is_self_referencing,
    null AS is_identity,
    null AS identity_generation,
    null AS identity_start,
    null AS identity_increment,
    null AS identity_maximum,
    null AS identity_minimum,
    null AS identity_cyle,
    null AS is_generated,
    null AS generation_expression,
    'NO' AS is_updateable
FROM glare_catalog.external_columns c
    JOIN glare_catalog.tables t
    ON c.table_oid = t.oid
    JOIN glare_catalog.schemas s
    ON c.schema_oid = s.oid
;",
});

// Postgres catalog tables.
//
// See <https://www.postgresql.org/docs/current/catalogs.html>

pub static PG_AM: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_am",
    sql: "
SELECT
    0 AS oid,
    'scan' AS amname,
    null AS amhandler,
    't' AS amtype",
});

pub static PG_ATTRIBUTE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_attribute",
    sql: "
SELECT
    c.table_oid AS attrelid,
    c.column_name AS attname,
    null AS atttypeid,
    null AS attstattarget,
    null AS attlen,
    null AS attnum,
    null AS attndims,
    null AS attcacheoff,
    null AS atttypmod,
    null AS attbyval,
    null AS attalign,
    null AS attstorage,
    null AS attcompression,
    null AS attnotnull,
    null AS atthasdef,
    null AS atthasmissing,
    null AS attidentity,
    null AS attgenerated,
    null AS attisdropped,
    null AS attislocal,
    null AS attinhcount,
    null AS attcollation,
    null AS attacl,
    null AS attoptions,
    null AS attfdwoptions,
    null AS attmissingval
FROM glare_catalog.columns c",
});

pub static PG_CLASS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_class",
    sql: "
SELECT
    t.oid AS oid,
    t.table_name AS relname,
    t.schema_oid AS relnamespace,
    0 AS reltype,
    null AS reloftype,
    null AS relowner,
    null AS relam,
    null AS relfilenode,
    null AS reltablespace,
    null AS relpages,
    null AS reltyples,
    null AS relallvisible,
    null AS reltoastrelid,
    null AS relhasindex,
    null AS relisshared,
    null AS relpersistence,
    'r' AS relkind,
    null AS relnatts,
    null AS relchecks,
    null AS relhasrules,
    null AS relhastriggers,
    null AS relhassubclass,
    null AS relrowsecurity,
    null AS relforcerowsecurity,
    null AS relispopulated,
    null AS relreplident,
    null AS relispartition,
    null AS relrewrite,
    null AS relfrozenxid,
    null AS relminxid,
    null AS relacl,
    null AS reloptions,
    null AS relpartbound
FROM glare_catalog.tables t",
});

pub static PG_NAMESPACE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_namespace",
    sql: "
SELECT
    s.oid AS oid,
    s.schema_name AS nspname,
    0 AS nspowner,
    null AS nspacl
FROM glare_catalog.schemas s",
});

pub static PG_DESCRIPTION: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_description",
    sql: "
SELECT
    1 AS objoid,
    2 AS classoid,
    3 AS objsubid,
    4 AS description
FROM (VALUES (NULL, NULL, NULL, NULL)) WHERE false", // Create empty table for now.
});

impl BuiltinView {
    pub fn builtins() -> Vec<&'static BuiltinView> {
        vec![
            &INFORMATION_SCHEMA_SCHEMATA,
            &INFORMATION_SCHEMA_TABLES,
            &INFORMATION_SCHEMA_COLUMNS,
            &PG_AM,
            &PG_ATTRIBUTE,
            &PG_CLASS,
            &PG_NAMESPACE,
            &PG_DESCRIPTION,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn builtin_schema_oid_range() {
        let mut oids = HashSet::new();
        for schema in BuiltinSchema::builtins() {
            assert!(schema.oid < FIRST_NON_SCHEMA_ID);
            assert!(schema.oid >= FIRST_GLAREDB_BUILTIN_ID);
            assert!(oids.insert(schema.oid), "duplicate oid: {}", schema.oid);
        }
    }

    #[test]
    fn builtin_unique_schema_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinSchema::builtins() {
            assert!(names.insert(builtin.name.to_string()))
        }
    }

    #[test]
    fn builtin_unique_view_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinView::builtins() {
            let name = format!(
                "{}.{}",
                builtin.schema.to_string(),
                builtin.name.to_string()
            );
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }

    #[test]
    fn builtin_unique_table_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinTable::builtins() {
            let name = format!(
                "{}.{}",
                builtin.schema.to_string(),
                builtin.name.to_string()
            );
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }
}
