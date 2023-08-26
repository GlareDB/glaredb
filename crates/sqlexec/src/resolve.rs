use datafusion::{datasource::MemTable, sql::TableReference};
use protogen::metastore::types::catalog::{DatabaseEntry, TableEntry};
use sqlbuiltins::builtins::DEFAULT_CATALOG;

use crate::metastore::catalog::{SessionCatalog, TempCatalog};

#[derive(Debug, Clone, thiserror::Error)]
#[error("failed to resolve: {0}")]
pub struct ResolveError(String);

type Result<T, E = ResolveError> = std::result::Result<T, E>;

pub enum ResolvedTableEntry {
    /// We have a table in the catalog.
    Table(TableEntry),
    /// We have a table in the temporary session-scoped catalog.
    TempTable(TableEntry),
    /// Table reference points to external database, and we don't know if the
    /// table exists or not. The external database needs to be contacted.
    NeedsExternalResolution(DatabaseEntry),
}

/// Resolves entries in the catalog and session temp objects while taking into
/// account the search path.
pub struct EntryResolver<'a> {
    /// Catalog to lookup entries in.
    pub catalog: &'a SessionCatalog,
    /// Temp objects scoped to a session.
    pub temp_objects: &'a TempCatalog,
    /// Schemas to use when looking up a table.
    pub schema_search_path: &'a [String],
}

impl<'a> EntryResolver<'a> {
    pub fn resolve_table_entry_from_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Result<ResolvedTableEntry> {
        match &reference {
            TableReference::Bare { table } => {
                // Check for temp table first. This matches Postgres behavior.
                if let Some(table) = self.temp_objects.resolve_temp_table(&table) {
                    return Ok(ResolvedTableEntry::TempTable(table));
                }

                // Iterate through all schemas in the search path looking for
                // our table.
                for schema in self.schema_search_path.iter() {
                    if let Some(ent) = self.catalog.resolve_table(DEFAULT_CATALOG, schema, table) {
                        return Ok(ResolvedTableEntry::Table(ent.clone()));
                    }
                    // Continue on, trying the next schema.
                }
            }
            TableReference::Partial { schema, table } => {
                if let Some(ent) = self.catalog.resolve_table(DEFAULT_CATALOG, schema, table) {
                    return Ok(ResolvedTableEntry::Table(ent.clone()));
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
                    return Ok(ResolvedTableEntry::NeedsExternalResolution(db_ent.clone()));
                }

                // Otherwise just try to get the full qualified table.
                if let Some(ent) = self.catalog.resolve_table(catalog, schema, table) {
                    return Ok(ResolvedTableEntry::Table(ent.clone()));
                }
            }
        }

        Err(ResolveError(format!("failed to find table: {reference}")))
    }
}
