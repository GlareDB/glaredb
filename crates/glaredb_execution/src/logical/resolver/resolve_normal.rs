use std::collections::HashMap;
use std::sync::Arc;

use glaredb_error::{DbError, Result};
use glaredb_parser::ast;
use tracing::error;

use super::ResolveContext;
use super::resolved_table::{
    ResolvedTableOrCteReference,
    ResolvedTableReference,
    ResolvedViewReference,
    UnresolvedTableReference,
};
use crate::catalog::context::{DatabaseContext, SYSTEM_CATALOG};
use crate::catalog::database::Database;
use crate::catalog::entry::{CatalogEntry, CatalogEntryInner, CatalogEntryType};
use crate::catalog::memory::MemorySchema;
use crate::catalog::system::BUILTIN_SCHEMA;
use crate::catalog::{Catalog, Schema};
use crate::expr;
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::TableFunctionInput;

pub fn create_user_facing_resolve_err(
    schema_ent: Option<&MemorySchema>,
    object_types: &[CatalogEntryType],
    name: &str,
) -> DbError {
    // Find similar function to include in error message.
    let similar = match schema_ent {
        Some(schema_ent) => match schema_ent.find_similar_entry(object_types, name) {
            Ok(maybe_similar) => maybe_similar,
            Err(e) => {
                // Error shouldn't happen, but if it does, it shouldn't be user-facing.
                error!(%e, %name, "failed to find similar entry to include in error message");
                None
            }
        },
        None => None,
    };

    // "table"
    // "scalar function or aggregate function"
    let formatted_object_types = object_types
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(" or ");

    match similar {
        Some(similar) => DbError::new(format!(
            "Cannot resolve {} with name '{}', did you mean '{}'?",
            formatted_object_types, name, similar.name,
        )),
        None => DbError::new(format!(
            "Cannot resolve {} with name '{}'",
            formatted_object_types, name
        )),
    }
}

#[derive(Debug)]
pub enum MaybeResolvedTable {
    /// We have the table, and know everything about the table.
    Resolved(ResolvedTableOrCteReference),
    /// We have a catalog that might contain the table, but additional
    /// resolution needs to happen to ensure that's the case (hybrid).
    UnresolvedWithCatalog(UnresolvedTableReference),
    /// Table has no candidate catalogs it could be in.
    Unresolved,
}

// TODO: Search path
#[derive(Debug)]
pub struct NormalResolver<'a> {
    pub context: &'a DatabaseContext,
}

impl<'a> NormalResolver<'a> {
    pub fn new(context: &'a DatabaseContext) -> Self {
        NormalResolver { context }
    }

    /// Resolve a table function.
    pub fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Option<TableFunctionSet>> {
        // TODO: Search path.
        let [catalog, schema, name] = match reference.0.len() {
            1 => [
                SYSTEM_CATALOG.to_string(),
                BUILTIN_SCHEMA.to_string(),
                reference.0[0].as_normalized_string(),
            ],
            2 => {
                let name = reference.0[1].as_normalized_string();
                let schema = reference.0[0].as_normalized_string();
                [SYSTEM_CATALOG.to_string(), schema, name]
            }
            3 => {
                let name = reference.0[2].as_normalized_string();
                let schema = reference.0[1].as_normalized_string();
                let catalog = reference.0[0].as_normalized_string();
                [catalog, schema, name]
            }
            _ => {
                return Err(DbError::new(
                    "Unexpected number of identifiers in table function reference",
                ));
            }
        };

        let schema_ent = match self
            .context
            .require_get_database(&catalog)?
            .catalog
            .get_schema(&schema)?
        {
            Some(ent) => ent,
            None => return Ok(None),
        };

        match schema_ent.get_table_function(&name)? {
            Some(entry) => Ok(Some(entry.try_as_table_function_entry()?.function)),
            _ => Ok(None),
        }
    }

    pub fn require_resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<TableFunctionSet> {
        self.resolve_table_function(reference)?.ok_or_else(|| {
            DbError::new(format!(
                "Missing table function for reference '{}'",
                reference
            ))
        })
    }

    /// Resolve a table or cte.
    pub async fn resolve_table_or_cte(
        &self,
        reference: &ast::ObjectReference,
        resolve_context: &ResolveContext,
    ) -> Result<MaybeResolvedTable> {
        // TODO: Seach path.
        let [catalog, schema, table] = match reference.0.len() {
            1 => {
                let name = reference.0[0].as_normalized_string();

                // Check bind data for cte that would satisfy this reference.
                if let Some(cte) = resolve_context.find_cte(&name) {
                    return Ok(MaybeResolvedTable::Resolved(
                        ResolvedTableOrCteReference::Cte(cte.name.clone()),
                    ));
                }

                // Otherwise continue with trying to resolve from the catalogs.
                ["temp".to_string(), "temp".to_string(), name]
            }
            2 => {
                let table = reference.0[1].as_normalized_string();
                let schema = reference.0[0].as_normalized_string();
                ["temp".to_string(), schema, table]
            }
            3 => {
                let table = reference.0[2].as_normalized_string();
                let schema = reference.0[1].as_normalized_string();
                let catalog = reference.0[0].as_normalized_string();
                [catalog, schema, table]
            }
            _ => {
                return Err(DbError::new(
                    "Unexpected number of identifiers in table reference",
                ));
            }
        };

        let database = self.context.require_get_database(&catalog)?;

        // Try reading from in-memory catalog first.
        if let Some(entry) = self.resolve_from_memory_catalog(database, &schema, &table)? {
            match &entry.entry {
                CatalogEntryInner::Table(table_ent) => {
                    // Base table, get the table scan function and use that.
                    //
                    // Arguments are (catalog, schema, table)
                    let inputs = TableFunctionInput {
                        positional: vec![
                            expr::lit(catalog.clone()).into(),
                            expr::lit(schema.clone()).into(),
                            expr::lit(table.clone()).into(),
                        ],
                        named: HashMap::new(),
                    };

                    let planned =
                        expr::bind_table_scan_function(&table_ent.function, self.context, inputs)
                            .await?;

                    return Ok(MaybeResolvedTable::Resolved(
                        ResolvedTableOrCteReference::Table(ResolvedTableReference {
                            catalog,
                            schema,
                            entry,
                            scan_function: planned,
                        }),
                    ));
                }
                CatalogEntryInner::View(_) => {
                    return Ok(MaybeResolvedTable::Resolved(
                        ResolvedTableOrCteReference::View(ResolvedViewReference {
                            catalog,
                            schema,
                            entry,
                        }),
                    ));
                }
                _ => {
                    return Err(DbError::new(format!(
                        "Unexpected catalog entry type: {:?}",
                        entry.entry_type()
                    )));
                }
            }
        }

        // Nothing to load from. Return None instead of an error to the
        // remote side in hybrid execution to potentially load from
        // external source.
        Ok(MaybeResolvedTable::UnresolvedWithCatalog(
            UnresolvedTableReference {
                catalog: catalog.to_string(),
                reference: reference.clone(),
                attach_info: database.attach_info.clone(),
            },
        ))
    }

    fn resolve_from_memory_catalog(
        &self,
        database: &Database,
        schema: &str,
        table: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let schema_ent = match database.catalog.get_schema(schema)? {
            Some(ent) => ent,
            None => return Ok(None),
        };

        schema_ent.get_table_or_view(table)
    }

    pub async fn require_resolve_table_or_cte(
        &self,
        reference: &ast::ObjectReference,
        resolve_context: &ResolveContext,
    ) -> Result<ResolvedTableOrCteReference> {
        match self
            .resolve_table_or_cte(reference, resolve_context)
            .await?
        {
            MaybeResolvedTable::Resolved(table) => Ok(table),
            _ => Err(DbError::new(format!(
                "Missing table or view for reference '{}'",
                reference
            ))),
        }
    }
}
