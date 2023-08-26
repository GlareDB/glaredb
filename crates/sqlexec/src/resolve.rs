use datafusion::{datasource::MemTable, sql::TableReference};
use protogen::metastore::types::catalog::{CatalogEntry, DatabaseEntry, TableEntry};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use std::sync::Arc;

use crate::{
    context::local::LocalSessionContext,
    metastore::catalog::{SessionCatalog, TempCatalog},
};

#[derive(Debug, Clone, thiserror::Error)]
#[error("failed to resolve: {0}")]
pub struct ResolveError(String);

type Result<T, E = ResolveError> = std::result::Result<T, E>;

pub enum ResolvedEntry {
    /// We have an entry in the catalog.
    Entry(CatalogEntry),
    /// Reference points to external database, and we don't know if the entry
    /// exists or not. The external database needs to be contacted.
    NeedsExternalResolution {
        db_ent: DatabaseEntry,
        schema: String,
        name: String,
    },
}

impl ResolvedEntry {
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
pub struct EntryResolver<'a> {
    /// Catalog to lookup entries in.
    pub catalog: &'a SessionCatalog,
    /// Temp objects scoped to a session.
    pub temp_objects: Arc<TempCatalog>,
    /// Schemas to use when looking up a table.
    pub schema_search_path: Vec<String>,
}

impl<'a> EntryResolver<'a> {
    pub fn from_context(ctx: &'a LocalSessionContext) -> Self {
        EntryResolver {
            catalog: ctx.get_session_catalog(),
            temp_objects: ctx.get_temp_objects(),
            schema_search_path: ctx.get_session_vars().implicit_search_path(),
        }
    }

    pub fn resolve_entry_from_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Result<ResolvedEntry> {
        match &reference {
            TableReference::Bare { table } => {
                // Check for temp table first. This matches Postgres behavior.
                if let Some(table) = self.temp_objects.resolve_temp_table(&table) {
                    return Ok(ResolvedEntry::Entry(CatalogEntry::Table(table)));
                }

                // Iterate through all schemas in the search path looking for
                // our table.
                for schema in self.schema_search_path.iter() {
                    if let Some(ent) = self.catalog.resolve_table(DEFAULT_CATALOG, schema, table) {
                        return Ok(ResolvedEntry::Entry(CatalogEntry::Table(ent.clone())));
                    }
                    // Continue on, trying the next schema.
                }
            }
            TableReference::Partial { schema, table } => {
                if let Some(ent) = self.catalog.resolve_table(DEFAULT_CATALOG, schema, table) {
                    return Ok(ResolvedEntry::Entry(CatalogEntry::Table(ent.clone())));
                }
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                // If catalog is anything but "default", we know we need to do
                // external resolution since we don't store info about
                // individual tables.
                if catalog != DEFAULT_CATALOG {
                    let db_ent = self.catalog.resolve_database(&catalog).ok_or_else(|| {
                        ResolveError(format!("unable to find database entry for '{catalog}'"))
                    })?;
                    return Ok(ResolvedEntry::NeedsExternalResolution {
                        db_ent: db_ent.clone(),
                        schema: schema.clone().into_owned(),
                        name: table.clone().into_owned(),
                    });
                }

                // Otherwise just try to get the full qualified table.
                if let Some(ent) = self.catalog.resolve_table(catalog, schema, table) {
                    return Ok(ResolvedEntry::Entry(CatalogEntry::Table(ent.clone())));
                }
            }
        }

        Err(ResolveError(format!("failed to find table: {reference}")))
    }
}
