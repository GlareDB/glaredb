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

use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use once_cell::sync::Lazy;
use pgrepr::oid::FIRST_GLAREDB_BUILTIN_ID;
use protogen::metastore::types::options::InternalColumnDefinition;
use std::sync::Arc;

/// The default catalog that exists in all GlareDB databases.
pub const DEFAULT_CATALOG: &str = "default";

/// Default schema that's created on every startup.
pub const DEFAULT_SCHEMA: &str = "public";

/// Internal schema for system tables.
pub const INTERNAL_SCHEMA: &str = "glare_catalog";

pub const INFORMATION_SCHEMA: &str = "information_schema";
pub const POSTGRES_SCHEMA: &str = "pg_catalog";

/// Schema to store temporary objects (only valid for current session).
pub const CURRENT_SESSION_SCHEMA: &str = "current_session";

/// First oid available for other builtin objects that don't have a stable OID.
///
/// Builtin schemas have stable OIDs since everything (builtin and user objects)
/// depends on a schema.
///
/// Builtin tables have stable OIDs since some depend on data written to disk.
///
/// First glaredb builtin OID: 16384
/// First user object OID: 20000
///
/// This means we have ~3600 OIDs to play with for builtin objects. Note that
/// once a builtin object is given a stable OID, it **must not** be changed ever
/// (unless you're the person willing to write a migration system).
///
/// Stable OIDs should also not be reused. E.g. if we end up removing a table in
/// the future, we should default to not using that OID in the future (there are
/// cases where an OID is safe to reuse, but that should be determined
/// case-by-case).
///
/// General OID ranges:
/// Builtin schemas: 16385 - 16400 (16 OIDs)
/// Builtin tables: 16401 - 16500 (100 OIDs)
///
/// Constructing the builtin catalog happens in metastore, and errors on
/// encountering a duplicated OID. A test exists to ensure it's able to be
/// built.
pub const FIRST_NON_STATIC_OID: u32 = FIRST_GLAREDB_BUILTIN_ID + 116;

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
// TODO: Do we want something to indicate if a table is persisted in delta?
#[derive(Debug, Clone)]
pub struct BuiltinTable {
    pub schema: &'static str,
    pub name: &'static str,
    pub columns: Vec<InternalColumnDefinition>,
    pub oid: u32,
}

pub static GLARE_DATABASES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "databases",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("external", DataType::Boolean, false),
        ("datasource", DataType::Utf8, false),
        ("access_mode", DataType::Utf8, false), // `SourceAccessMode::as_str()`
    ]),
    oid: 16401,
});

pub static GLARE_TUNNELS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "tunnels",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("tunnel_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("tunnel_type", DataType::Utf8, false),
    ]),
    oid: 16402,
});

pub static GLARE_CREDENTIALS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "credentials",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("credentials_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("provider", DataType::Utf8, false),
        ("comment", DataType::Utf8, false),
    ]),
    oid: 16403,
});

pub static GLARE_SCHEMAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "schemas",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_oid", DataType::UInt32, false),
        ("database_name", DataType::Utf8, false),
        ("schema_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
    ]),
    oid: 16404,
});

pub static GLARE_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "tables",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("schema_name", DataType::Utf8, false),
        ("table_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("external", DataType::Boolean, false),
        ("datasource", DataType::Utf8, false),
        ("access_mode", DataType::Utf8, false), // `SourceAccessMode::as_str()`
    ]),
    oid: 16405,
});

pub static GLARE_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "views",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("database_oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("schema_name", DataType::Utf8, false),
        ("view_name", DataType::Utf8, false),
        ("builtin", DataType::Boolean, false),
        ("sql", DataType::Utf8, false),
    ]),
    oid: 16406,
});

pub static GLARE_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "columns",
    columns: InternalColumnDefinition::from_tuples([
        ("schema_oid", DataType::UInt32, false),
        ("table_oid", DataType::UInt32, false),
        ("table_name", DataType::Utf8, false),
        ("column_name", DataType::Utf8, false),
        ("column_ordinal", DataType::UInt32, false),
        ("data_type", DataType::Utf8, false),
        ("is_nullable", DataType::Boolean, false),
    ]),
    oid: 16407,
});

pub static GLARE_FUNCTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "functions",
    columns: InternalColumnDefinition::from_tuples([
        ("oid", DataType::UInt32, false),
        ("schema_oid", DataType::UInt32, false),
        ("function_name", DataType::Utf8, false),
        ("function_type", DataType::Utf8, false), // table, scalar, aggregate
        (
            "parameters",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
            false,
        ),
        ("builtin", DataType::Boolean, false),
        ("example", DataType::Utf8, true),
        ("description", DataType::Utf8, true),
    ]),
    oid: 16408,
});

pub static GLARE_SSH_KEYS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "ssh_keys",
    columns: InternalColumnDefinition::from_tuples([
        ("ssh_tunnel_oid", DataType::UInt32, false),
        ("ssh_tunnel_name", DataType::Utf8, false),
        ("public_key", DataType::Utf8, false),
    ]),
    oid: 16409,
});

pub static GLARE_DEPLOYMENT_METADATA: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "deployment_metadata",
    columns: InternalColumnDefinition::from_tuples([
        ("key", DataType::Utf8, false),
        ("value", DataType::Utf8, false),
    ]),
    oid: 16410,
});

/// Cached table metadata for external databases.
///
/// This stores information for all tables, and all columns for each table.
///
/// The cached data lives in an on-disk (delta) table alongside user table data.
///
// TODO: Do we want to store columns in a separate table?
pub static GLARE_CACHED_EXTERNAL_DATABASE_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "cached_external_database_tables",
    columns: InternalColumnDefinition::from_tuples([
        // External database this entry is for.
        ("database_oid", DataType::UInt32, false),
        // Schema name (in external database).
        ("schema_name", DataType::Utf8, false),
        // Table name (in external database).
        ("table_name", DataType::Utf8, false),
        // Column name (in external database).
        ("column_name", DataType::Utf8, false),
        ("data_type", DataType::Utf8, false),
    ]),
    oid: 16411,
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
                .collect::<Vec<_>>(),
        )
    }

    /// Return a vector of all builtin tables.
    pub fn builtins() -> Vec<&'static BuiltinTable> {
        vec![
            &GLARE_DATABASES,
            &GLARE_TUNNELS,
            &GLARE_CREDENTIALS,
            &GLARE_SCHEMAS,
            &GLARE_VIEWS,
            &GLARE_TABLES,
            &GLARE_COLUMNS,
            &GLARE_FUNCTIONS,
            &GLARE_SSH_KEYS,
            &GLARE_DEPLOYMENT_METADATA,
            &GLARE_CACHED_EXTERNAL_DATABASE_TABLES,
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

pub static SCHEMA_CURRENT_SESSION: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: CURRENT_SESSION_SCHEMA,
    oid: 16389,
});

impl BuiltinSchema {
    pub fn builtins() -> Vec<&'static BuiltinSchema> {
        vec![
            &SCHEMA_INTERNAL,
            &SCHEMA_DEFAULT,
            &SCHEMA_INFORMATION,
            &SCHEMA_POSTGRES,
            &SCHEMA_CURRENT_SESSION,
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

pub static GLARE_EXTERNAL_DATASOURCES: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INTERNAL_SCHEMA,
    name: "external_datasources",
    sql: "
WITH datasources(oid, name, datasource, object_type, external, access_mode) AS (
    SELECT oid,
           database_name,
           datasource,
           'database',
           external,
           access_mode
    FROM glare_catalog.databases
    UNION
    SELECT oid,
           table_name,
           datasource,
           'table',
           external,
           access_mode
    FROM glare_catalog.tables
)
SELECT * FROM datasources WHERE external = true",
});

// Information schema tables.
//
// See <https://www.postgresql.org/docs/current/information-schema.html>

pub static INFORMATION_SCHEMA_SCHEMATA: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "schemata",
    sql: "
SELECT
    database_name AS catalog_name,
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
        d.database_name AS table_catalog,
        t.schema_name AS table_schema,
        t.table_name AS table_name,
        'BASE TABLE' AS table_type,
        null AS self_referencing_column_name,
        null AS reference_generation,
        null AS user_defined_type_catalog,
        null AS user_defined_type_schema,
        null AS user_defined_type_name,
        'NO' AS is_insertable_into,
        'NO' AS is_typed,
        null AS commit_action
    FROM glare_catalog.tables t INNER JOIN glare_catalog.databases d ON t.database_oid = d.oid
    UNION ALL
    SELECT
        d.database_name AS table_catalog,
        v.schema_name AS table_schema,
        v.view_name AS table_name,
        'VIEW' AS table_type,
        null AS self_referencing_column_name,
        null AS reference_generation,
        null AS user_defined_type_catalog,
        null AS user_defined_type_schema,
        null AS user_defined_type_name,
        'NO' AS is_insertable_into,
        'NO' AS is_typed,
        null AS commit_action
    FROM glare_catalog.views v INNER JOIN glare_catalog.databases d ON v.database_oid = d.oid
)",
});

pub static INFORMATION_SCHEMA_COLUMNS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: INFORMATION_SCHEMA,
    name: "columns",
    sql: "
SELECT
    d.database_name AS table_catalog,
    s.schema_name AS table_schema,
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.column_ordinal + 1 AS ordinal_position,
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
INNER JOIN glare_catalog.schemas s ON c.schema_oid = s.oid
INNER JOIN glare_catalog.databases d ON s.database_oid = d.oid
",
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
    null           as attacl,
    ' '            as attalign,
    false          as attbyval,
    0              as attcacheoff,
    0              as attcollation,
    ' '            as attcompression,
    null           as attfdwoptions,
    ' '            as attgenerated,
    false          as atthasdef,
    false          as atthasmissing,
    ' '            as attidentity,
    0              as attinhcount,
    false          as attisdropped,
    false          as attislocal,
    0::smallint    as attlen,
    null           as attmissingval,
    ''             as attname,
    0              as attndims,
    false          as attnotnull,
    0::smallint    as attnum,
    null           as attoptions,
    0              as attrelid,
    0              as attstattarget,
    ' '            as attstorage,
    0              as atttypid,
    0              as atttypmod
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

pub static PG_DATABASE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_database",
    sql: "
SELECT
    oid as oid,
    database_name as datname,
    0 as datdba,
    0 as encoding,
    'c' as datlocprovider,
    false as datistemplate,
    true as datallowconn,
    -1 as datconnlimit,
    0 as datfrozenxid,
    0 as datminmxid,
    0 as dattablespace,
    '' as datcollate,
    '' as datctype,
    '' as daticulocal,
    '' as datcollversion,
    [] as datacl
FROM glare_catalog.databases;
",
});

pub static PG_TABLE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_table",
    sql: "
SELECT
    schema_name as schemaname,
    table_name as tablename,
    '' as tableowner,
    '' as tablespace,
    false as hasindexes,
    false as hasrules,
    false as hastriggers,
    false as rowsecurity
FROM glare_catalog.tables;
",
});

pub static PG_VIEWS: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_views",
    sql: "
SELECT
    schema_name as schemaname,
    view_name as viewname,
    '' as viewowner,
    sql as definition
FROM glare_catalog.views;
",
});
pub static PG_TYPE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    schema: POSTGRES_SCHEMA,
    name: "pg_type",
    sql: "
SELECT 

null as typacl,
0 as typndims,
0 as typcollation,
0 as oid,
0 as typnamespace,
0 as typowner,
0 as typlen,
false as typbyval,
'r' as typtype,
'r' as typcategory,
false as typispreferred,
false as typisdefined,
'r' as typdelim,
0 as typrelid,
0 as typsubscript,
0 as typelem,
0 as typarray,
0 as typinput,
0 as typoutput,
0 as typreceive,
0 as typsend,
0 as typmodin,
0 as typmodout,
0 as typanalyze,
'r' as typalign,
'r' as typstorage,
false as typnotnull,
0 as typbasetype,
0 as typtypmod,
'r' as typname,
null as typdefault,
null as typdefaultbin
FROM (VALUES (NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL)) WHERE false",
});
impl BuiltinView {
    pub fn builtins() -> Vec<&'static BuiltinView> {
        vec![
            &GLARE_EXTERNAL_DATASOURCES,
            &INFORMATION_SCHEMA_SCHEMATA,
            &INFORMATION_SCHEMA_TABLES,
            &INFORMATION_SCHEMA_COLUMNS,
            &PG_AM,
            &PG_ATTRIBUTE,
            &PG_CLASS,
            &PG_NAMESPACE,
            &PG_DESCRIPTION,
            &PG_DATABASE,
            &PG_TABLE,
            &PG_VIEWS,
            &PG_TYPE,
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
            assert!(schema.oid < FIRST_NON_STATIC_OID);
            assert!(schema.oid >= FIRST_GLAREDB_BUILTIN_ID);
            assert!(oids.insert(schema.oid), "duplicate oid: {}", schema.oid);
        }
    }

    #[test]
    fn builtin_table_oid_range() {
        let mut oids = HashSet::new();
        for schema in BuiltinTable::builtins() {
            assert!(schema.oid < FIRST_NON_STATIC_OID);
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
            let name = format!("{}.{}", builtin.schema, builtin.name);
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }

    #[test]
    fn builtin_unique_table_names() {
        let mut names = HashSet::new();
        for builtin in BuiltinTable::builtins() {
            let name = format!("{}.{}", builtin.schema, builtin.name);
            assert!(names.insert(name.clone()), "duplicate name: {}", name);
        }
    }
}
