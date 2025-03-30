use glaredb_error::Result;

use super::create::{
    CreateAggregateFunctionInfo,
    CreateScalarFunctionInfo,
    CreateSchemaInfo,
    CreateTableFunctionInfo,
    CreateViewInfo,
    OnConflict,
};
use super::memory::MemoryCatalog;
use super::{Catalog, Schema};
use crate::functions::aggregate::builtin::BUILTIN_AGGREGATE_FUNCTION_SETS;
use crate::functions::scalar::builtin::BUILTIN_SCALAR_FUNCTION_SETS;
use crate::functions::table::builtin::BUILTIN_TABLE_FUNCTION_SETS;

pub const DEFAULT_SCHEMA: &str = "default";

/// Create a new system catalog with builtin functions.
///
/// The provided data source reigstry is used to initialize table functions for
/// reading external sources.
pub fn new_system_catalog() -> Result<MemoryCatalog> {
    let catalog = MemoryCatalog::empty();

    let builtin = catalog.create_schema(&CreateSchemaInfo {
        name: DEFAULT_SCHEMA.to_string(),
        on_conflict: OnConflict::Error,
    })?;

    let _pg_catalog = catalog.create_schema(&CreateSchemaInfo {
        name: "pg_catalog".to_string(),
        on_conflict: OnConflict::Error,
    })?;

    let _information_schema_catalog = catalog.create_schema(&CreateSchemaInfo {
        name: "information_schema".to_string(),
        on_conflict: OnConflict::Error,
    })?;

    // Add builtin scalars.
    for func in BUILTIN_SCALAR_FUNCTION_SETS.iter() {
        builtin.create_scalar_function(&CreateScalarFunctionInfo {
            name: func.name.to_string(),
            implementation: *func,
            on_conflict: OnConflict::Error,
        })?;

        for alias in func.aliases {
            builtin.create_scalar_function(&CreateScalarFunctionInfo {
                name: alias.to_string(),
                implementation: *func,
                on_conflict: OnConflict::Error,
            })?;
        }
    }

    // Add builtin aggregates.
    for func in BUILTIN_AGGREGATE_FUNCTION_SETS.iter() {
        builtin.create_aggregate_function(&CreateAggregateFunctionInfo {
            name: func.name.to_string(),
            implementation: *func,
            on_conflict: OnConflict::Error,
        })?;

        for alias in func.aliases {
            builtin.create_aggregate_function(&CreateAggregateFunctionInfo {
                name: alias.to_string(),
                implementation: *func,
                on_conflict: OnConflict::Error,
            })?;
        }
    }

    // Add builtin table functions.
    for func in BUILTIN_TABLE_FUNCTION_SETS.iter() {
        builtin.create_table_function(&CreateTableFunctionInfo {
            name: func.name.to_string(),
            implementation: *func,
            on_conflict: OnConflict::Error,
        })?;

        for alias in func.aliases {
            builtin.create_table_function(&CreateTableFunctionInfo {
                name: alias.to_string(),
                implementation: *func,
                on_conflict: OnConflict::Error,
            })?;
        }
    }

    // Add builtin views.
    for view in BUILTIN_VIEWS {
        builtin.create_view(&CreateViewInfo {
            name: view.name.to_string(),
            column_aliases: None,
            on_conflict: OnConflict::Error,
            query_string: view.view.to_string(),
        })?;
    }

    Ok(catalog)
}

/// All builtin views placed in the 'system.glare_catalog' schema.
pub const BUILTIN_VIEWS: &[BuiltinView] =
    &[SHOW_DATABASES_VIEW, SHOW_SCHEMAS_VIEW, SHOW_TABLES_VIEW];

/// Describes a builtin view.
#[derive(Debug)]
pub struct BuiltinView {
    pub name: &'static str,
    pub view: &'static str,
}

/// Shows all databases.
pub const SHOW_DATABASES_VIEW: BuiltinView = BuiltinView {
    name: "show_databases",
    view: "
SELECT database_name
FROM list_databases()
ORDER BY database_name;
",
};

/// Shows all schemas.
pub const SHOW_SCHEMAS_VIEW: BuiltinView = BuiltinView {
    name: "show_schemas",
    view: "
SELECT schema_name
FROM list_schemas()
ORDER BY schema_name;
",
};

/// Shows all tables and views.
pub const SHOW_TABLES_VIEW: BuiltinView = BuiltinView {
    name: "show_tables",
    view: "
WITH tables AS (
  SELECT table_name AS name
  FROM list_tables()
), views AS (
  SELECT view_name AS name
  FROM list_views()
), views_and_tables AS (
  SELECT * FROM tables UNION ALL SELECT * FROM views
)
SELECT name
FROM views_and_tables
ORDER BY name;
",
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_without_error() {
        // This will ensure we don't have duplicate names for functions.
        new_system_catalog().unwrap();
    }
}
