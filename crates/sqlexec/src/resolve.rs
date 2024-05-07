use std::borrow::Cow;

use catalog::session_catalog::SessionCatalog;
use datafusion::sql::TableReference;
use protogen::metastore::types::catalog::{CatalogEntry, DatabaseEntry, TableEntry};
use sqlbuiltins::builtins::{CURRENT_SESSION_SCHEMA, DEFAULT_CATALOG};

use crate::context::local::LocalSessionContext;

#[derive(Debug, Clone, thiserror::Error)]
#[error("failed to resolve: {0}")]
pub struct ResolveError(String);

type Result<T, E = ResolveError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum ResolvedEntry<'a> {
    /// We have an entry in the catalog.
    // NOTE: Not a reference since a `TableEntry` is created on-demand when
    // getting the entry for a temp table.
    Entry(CatalogEntry),
    /// Reference points to external database, and we don't know if the entry
    /// exists or not. The external database needs to be contacted.
    NeedsExternalResolution {
        db_ent: &'a DatabaseEntry,
        schema: Cow<'a, str>,
        name: Cow<'a, str>,
    },
}

impl<'a> ResolvedEntry<'a> {
    pub fn try_into_table_entry(self) -> Result<TableEntry> {
        match self {
            Self::Entry(CatalogEntry::Table(ent)) => Ok(ent),
            Self::Entry(ent) => Err(ResolveError(format!(
                "{} is not a table entry",
                ent.get_meta().name
            ))),
            Self::NeedsExternalResolution { .. } => Err(ResolveError(
                "entry type unknown, external resolution needed".to_string(),
            )),
        }
    }
}

/// Resolves entries in the catalog and session temp objects while taking into
/// account the search path.
// TODO: Remove Arc and Vec. (rethink how we handle storing things on
// datafusion's context).
#[derive(Debug)]
pub struct EntryResolver<'a> {
    /// Catalog to lookup entries in.
    pub catalog: &'a SessionCatalog,
    /// Schemas to use when looking up a table.
    pub schema_search_path: Vec<String>,
}

impl<'a> EntryResolver<'a> {
    pub fn from_context(ctx: &'a LocalSessionContext) -> Self {
        EntryResolver {
            catalog: ctx.get_session_catalog(),
            schema_search_path: ctx.get_session_vars().implicit_search_path(),
        }
    }

    pub fn resolve_entry_from_reference<'b: 'a>(
        &'a self,
        reference: TableReference<'b>,
    ) -> Result<ResolvedEntry> {
        match &reference {
            TableReference::Bare { table } => {
                // Check for temp table first. This matches Postgres behavior
                // where if there exists a persist table named "t1" and a temp
                // table also named "t1", the temp table is what gets used in
                // the query.
                if let Some(table) = self.catalog.get_temp_catalog().resolve_temp_table(table) {
                    return Ok(ResolvedEntry::Entry(CatalogEntry::Table(table)));
                }

                // builtin table functions should be independent of schema, pull them out of 'public' schema
                if let Some(function) = self.catalog.resolve_builtin_table_function(table) {
                    return Ok(ResolvedEntry::Entry(CatalogEntry::Function(function)));
                }

                // Iterate through all schemas in the search path looking for
                // our table.
                for schema in self.schema_search_path.iter() {
                    if let Some(ent) = self.catalog.resolve_entry(DEFAULT_CATALOG, schema, table) {
                        return Ok(ResolvedEntry::Entry(ent.clone()));
                    }
                    // Continue on, trying the next schema.
                }
            }
            TableReference::Partial { schema, table } => {
                // "current_session" references the temp catalog.
                if schema == CURRENT_SESSION_SCHEMA {
                    if let Some(table) = self.catalog.get_temp_catalog().resolve_temp_table(table) {
                        return Ok(ResolvedEntry::Entry(CatalogEntry::Table(table)));
                    }
                }

                if let Some(ent) = self.catalog.resolve_entry(DEFAULT_CATALOG, schema, table) {
                    return Ok(ResolvedEntry::Entry(ent.clone()));
                }
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                // if the catalog has an alias, we need to check if the
                // reference is to the alias and if so, resolve it to the
                // actual catalog name.
                if let Some(catalog_alias) = self.catalog.alias() {
                    if catalog == catalog_alias {
                        if let Some(ent) =
                            self.catalog.resolve_entry(DEFAULT_CATALOG, schema, table)
                        {
                            return Ok(ResolvedEntry::Entry(ent.clone()));
                        }
                    }
                }

                // If catalog is anything but "default", we know we need to do
                // external resolution since we don't store info about
                // individual tables.
                if catalog != DEFAULT_CATALOG {
                    let db_ent = self.catalog.resolve_database(catalog).ok_or_else(|| {
                        ResolveError(format!("unable to find database entry for '{catalog}'"))
                    })?;
                    return Ok(ResolvedEntry::NeedsExternalResolution {
                        db_ent,
                        schema: schema.clone(),
                        name: table.clone(),
                    });
                }

                // We also support referencing fully qualified temp table names.
                if schema == CURRENT_SESSION_SCHEMA {
                    if let Some(table) = self.catalog.get_temp_catalog().resolve_temp_table(table) {
                        return Ok(ResolvedEntry::Entry(CatalogEntry::Table(table)));
                    }
                }

                // Otherwise just try to get the full qualified entry.
                if let Some(ent) = self.catalog.resolve_entry(catalog, schema, table) {
                    return Ok(ResolvedEntry::Entry(ent.clone()));
                }
            }
        }

        Err(ResolveError(format!("failed to find table: {reference}")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use catalog::session_catalog::ResolveConfig;
    use protogen::metastore::types::catalog::{
        CatalogState,
        DeploymentMetadata,
        EntryMeta,
        EntryType,
        SchemaEntry,
        SourceAccessMode,
    };
    use protogen::metastore::types::options::TableOptionsV0;

    use super::*;

    fn new_test_catalog_state() -> CatalogState {
        let entries = [
            // Schemas
            CatalogEntry::Schema(SchemaEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Schema,
                    id: 1,
                    parent: 0,
                    name: "my_schema1".to_string(),
                    builtin: false,
                    external: false,
                    is_temp: false,
                },
            }),
            CatalogEntry::Schema(SchemaEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Schema,
                    id: 2,
                    parent: 0,
                    name: "my_schema2".to_string(),
                    builtin: false,
                    external: false,
                    is_temp: false,
                },
            }),
            // Tables
            CatalogEntry::Table(TableEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Schema,
                    id: 3,
                    parent: 1,
                    name: "table1".to_string(),
                    builtin: false,
                    external: false,
                    is_temp: false,
                },
                options: TableOptionsV0::new_internal(Vec::new()),
                tunnel_id: None,
                access_mode: SourceAccessMode::ReadWrite,
                columns: None,
            }),
            // Tables
            CatalogEntry::Table(TableEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Schema,
                    id: 4,
                    parent: 2,
                    name: "table2".to_string(),
                    builtin: false,
                    external: false,
                    is_temp: false,
                },
                options: TableOptionsV0::new_internal(Vec::new()),
                tunnel_id: None,
                access_mode: SourceAccessMode::ReadWrite,
                columns: None,
            }),
        ];

        let entries: HashMap<_, _> = entries
            .into_iter()
            .map(|ent| (ent.get_meta().id, ent))
            .collect();

        CatalogState {
            version: 0,
            entries,
            deployment: DeploymentMetadata { storage_size: 0 },
            catalog_version: 0,
        }
    }

    #[test]
    fn resolve_with_table_name() {
        let session_catalog = SessionCatalog::new(
            Arc::new(new_test_catalog_state()),
            ResolveConfig {
                default_schema_oid: 0,
                session_schema_oid: 0,
            },
        );

        let resolver = EntryResolver {
            catalog: &session_catalog,
            schema_search_path: vec!["my_schema1".to_string()],
        };

        let ent = resolver
            .resolve_entry_from_reference(TableReference::Bare {
                table: "table1".into(),
            })
            .unwrap();
        let table_ent = ent.try_into_table_entry().unwrap();

        assert_eq!("table1", table_ent.meta.name);
    }

    #[test]
    fn resolve_fully_qualified_with_default() {
        let session_catalog = SessionCatalog::new(
            Arc::new(new_test_catalog_state()),
            ResolveConfig {
                default_schema_oid: 0,
                session_schema_oid: 0,
            },
        );

        let resolver = EntryResolver {
            catalog: &session_catalog,
            schema_search_path: vec!["my_schema1".to_string()],
        };

        let ent = resolver
            .resolve_entry_from_reference(TableReference::full("default", "my_schema2", "table2"))
            .unwrap();
        let table_ent = ent.try_into_table_entry().unwrap();

        assert_eq!("table2", table_ent.meta.name);
    }

    #[test]
    fn resolve_fully_qualified_with_alias() {
        let session_catalog = SessionCatalog::new_with_alias(
            Arc::new(new_test_catalog_state()),
            ResolveConfig {
                default_schema_oid: 0,
                session_schema_oid: 0,
            },
            "hello3/crimson_snow",
        );

        let resolver = EntryResolver {
            catalog: &session_catalog,
            schema_search_path: vec!["my_schema1".to_string()],
        };

        let ent = resolver
            .resolve_entry_from_reference(TableReference::full(
                "hello3/crimson_snow",
                "my_schema2",
                "table2",
            ))
            .unwrap();
        let table_ent = ent.try_into_table_entry().unwrap();

        assert_eq!("table2", table_ent.meta.name);
    }
}
