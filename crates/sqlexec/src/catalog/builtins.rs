use crate::catalog::constants::{
    DEFAULT_SCHEMA, INFORMATION_SCHEMA, INTERNAL_SCHEMA, POSTGRES_SCHEMA,
};
use crate::catalog::entry::ColumnDefinition;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use once_cell::sync::Lazy;

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
    columns: ColumnDefinition::from_arrow_fields(&[
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("view_name", DataType::Utf8, false),
        Field::new("sql", DataType::Utf8, false),
    ]),
});

pub static GLARE_SCHEMAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "schemas",
    columns: ColumnDefinition::from_arrow_fields(&[Field::new(
        "schema_name",
        DataType::Utf8,
        false,
    )]),
});

pub static GLARE_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "tables",
    columns: ColumnDefinition::from_arrow_fields(&[
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_count", DataType::UInt32, false),
        Field::new("access", DataType::Utf8, false),
    ]),
});

pub static GLARE_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    schema: INTERNAL_SCHEMA,
    name: "columns",
    columns: ColumnDefinition::from_arrow_fields(&[
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_index", DataType::UInt32, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
    ]),
});

impl BuiltinTable {
    /// Check if this table matches the provided schema and name.
    pub fn matches(&self, schema: &str, name: &str) -> bool {
        self.schema == schema && self.name == name
    }

    /// Get the arrow schema for the builtin table.
    pub fn arrow_schema(&self) -> ArrowSchema {
        ArrowSchema::new(self.columns.iter().map(|field| field.into()).collect())
    }

    /// Return a vector of all builtin tables.
    pub fn builtins() -> Vec<&'static BuiltinTable> {
        vec![&GLARE_VIEWS, &GLARE_SCHEMAS, &GLARE_TABLES, &GLARE_COLUMNS]
    }
}

/// A builtin schema.
#[derive(Debug, Clone)]
pub struct BuiltinSchema {
    pub name: &'static str,
}

pub static SCHEMA_INFORMATION: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INFORMATION_SCHEMA,
});

pub static SCHEMA_INTERNAL: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: INTERNAL_SCHEMA,
});

pub static SCHEMA_POSTGRES: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: POSTGRES_SCHEMA,
});

pub static SCHEMA_DEFAULT: Lazy<BuiltinSchema> = Lazy::new(|| BuiltinSchema {
    name: DEFAULT_SCHEMA,
});

impl BuiltinSchema {
    pub fn builtins() -> Vec<&'static BuiltinSchema> {
        vec![
            &SCHEMA_INFORMATION,
            &SCHEMA_INTERNAL,
            &SCHEMA_POSTGRES,
            &SCHEMA_DEFAULT,
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

impl BuiltinView {
    pub fn builtins() -> Vec<&'static BuiltinView> {
        vec![&INFORMATION_SCHEMA_TABLES]
    }
}
