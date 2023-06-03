use crate::errors::{BuiltinError, Result};
use crate::{
    BuiltinDatabase, BuiltinSchema, BuiltinTable, BuiltinView, DATABASE_DEFAULT,
    DATABASE_PARENT_ID, FIRST_NON_SCHEMA_ID,
};
use metastoreproto::types::catalog::{
    CatalogEntry, CatalogState, DatabaseEntry, EntryMeta, EntryType, SchemaEntry, TableEntry,
    TunnelEntry, ViewEntry,
};
use metastoreproto::types::options::{
    DatabaseOptions, DatabaseOptionsInternal, TableOptions, TunnelOptions,
};
use once_cell::sync::Lazy;
use std::collections::HashMap;

/// A global builtin catalog. This is meant to be cloned for every database
/// catalog.
pub static BUILTIN_CATALOG: Lazy<BuiltinCatalog> = Lazy::new(|| BuiltinCatalog::new().unwrap());

/// Holds names to object ids for a single schema.
#[derive(Debug, Default, Clone)]
pub struct SchemaObjects {
    /// Maps names to ids in this schema.
    pub objects: HashMap<String, u32>,
}

/// Catalog with builtin objects. Used during database catalog initialization.
#[derive(Clone)]
pub struct BuiltinCatalog {
    /// All entries in the catalog.
    pub entries: HashMap<u32, CatalogEntry>,
    /// Map database names to their ids.
    pub database_names: HashMap<String, u32>,
    /// Map schema names to their ids.
    pub schema_names: HashMap<String, u32>,
    /// Map schema IDs to objects in the schema.
    pub schema_objects: HashMap<u32, SchemaObjects>,
}

impl BuiltinCatalog {
    /// Create a new builtin catalog.
    ///
    /// It is a programmer error if this fails to build.
    fn new() -> Result<BuiltinCatalog> {
        let mut entries = HashMap::new();
        let mut database_names = HashMap::new();
        let mut schema_names = HashMap::new();
        let mut schema_objects = HashMap::new();

        for database in BuiltinDatabase::builtins() {
            database_names.insert(database.name.to_string(), database.oid);
            entries.insert(
                database.oid,
                CatalogEntry::Database(DatabaseEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Database,
                        id: database.oid,
                        parent: DATABASE_PARENT_ID,
                        name: database.name.to_string(),
                        builtin: true,
                        external: false,
                    },
                    options: DatabaseOptions::Internal(DatabaseOptionsInternal {}),
                    tunnel_id: None,
                }),
            );
        }

        for schema in BuiltinSchema::builtins() {
            schema_names.insert(schema.name.to_string(), schema.oid);
            schema_objects.insert(schema.oid, SchemaObjects::default());
            entries.insert(
                schema.oid,
                CatalogEntry::Schema(SchemaEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Schema,
                        id: schema.oid,
                        parent: DATABASE_DEFAULT.oid,
                        name: schema.name.to_string(),
                        builtin: true,
                        external: false,
                    },
                }),
            );
        }

        // All the below items don't have stable ids.
        let mut oid = FIRST_NON_SCHEMA_ID;

        for table in BuiltinTable::builtins() {
            let schema_id = schema_names.get(table.schema).ok_or_else(|| {
                BuiltinError::CreateBuiltinCatalog(format!(
                    "Missing table schema: {}",
                    table.schema
                ))
            })?;
            entries.insert(
                oid,
                CatalogEntry::Table(TableEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Table,
                        id: oid,
                        parent: *schema_id,
                        name: table.name.to_string(),
                        builtin: true,
                        external: false,
                    },
                    options: TableOptions::new_internal(table.columns.clone()),
                    tunnel_id: None,
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .objects
                .insert(table.name.to_string(), oid);

            oid += 1;
        }

        for view in BuiltinView::builtins() {
            let schema_id = schema_names.get(view.schema).ok_or_else(|| {
                BuiltinError::CreateBuiltinCatalog(format!("Missing view schema: {}", view.schema))
            })?;
            entries.insert(
                oid,
                CatalogEntry::View(ViewEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::View,
                        id: oid,
                        parent: *schema_id,
                        name: view.name.to_string(),
                        builtin: true,
                        external: false,
                    },
                    sql: view.sql.to_string(),
                    columns: Vec::new(),
                }),
            );
            schema_objects
                .get_mut(schema_id)
                .unwrap()
                .objects
                .insert(view.name.to_string(), oid);

            oid += 1;
        }

        Ok(BuiltinCatalog {
            entries,
            database_names,
            schema_names,
            schema_objects,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_catalog_builds() {
        BuiltinCatalog::new().unwrap();
    }
}
