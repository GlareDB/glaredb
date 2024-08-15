use std::sync::Arc;

use crate::{
    database::{
        catalog::CatalogTx,
        catalog_entry::{CatalogEntry, CatalogEntryType},
        create::{CreateSchemaInfo, CreateTableInfo, OnConflict},
        memory_catalog::MemorySchema,
        Database, DatabaseContext,
    },
    functions::table::TableFunction,
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use tracing::error;

use super::{bound_table::BoundTableOrCteReference, BindData};

pub fn create_user_facing_resolve_err(
    tx: &CatalogTx,
    schema_ent: Option<&MemorySchema>,
    object_types: &[CatalogEntryType],
    name: &str,
) -> RayexecError {
    // Find similar function to include in error message.
    let similar = match schema_ent {
        Some(schema_ent) => match schema_ent.find_similar_entry(tx, object_types, name) {
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
        Some(similar) => RayexecError::new(format!(
            "Cannot resolve {} with name '{}', did you mean '{}'?",
            formatted_object_types, name, similar.entry.name,
        )),
        None => RayexecError::new(format!(
            "Cannot resolve {} with name '{}'",
            formatted_object_types, name
        )),
    }
}

// TODO: Search path
#[derive(Debug)]
pub struct Resolver<'a> {
    pub tx: &'a CatalogTx,
    pub context: &'a DatabaseContext,
}

impl<'a> Resolver<'a> {
    pub fn new(tx: &'a CatalogTx, context: &'a DatabaseContext) -> Self {
        Resolver { tx, context }
    }

    /// Resolve a table function.
    pub fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Option<Box<dyn TableFunction>>> {
        // TODO: Search path.
        let [catalog, schema, name] = match reference.0.len() {
            1 => [
                "system".to_string(),
                "glare_catalog".to_string(),
                reference.0[0].as_normalized_string(),
            ],
            2 => {
                let name = reference.0[1].as_normalized_string();
                let schema = reference.0[0].as_normalized_string();
                ["system".to_string(), schema, name]
            }
            3 => {
                let name = reference.0[2].as_normalized_string();
                let schema = reference.0[1].as_normalized_string();
                let catalog = reference.0[0].as_normalized_string();
                [catalog, schema, name]
            }
            _ => {
                return Err(RayexecError::new(
                    "Unexpected number of identifiers in table function reference",
                ))
            }
        };

        let schema_ent = match self
            .context
            .get_database(&catalog)?
            .catalog
            .get_schema(self.tx, &schema)?
        {
            Some(ent) => ent,
            None => return Ok(None),
        };

        if let Some(entry) = schema_ent.get_table_function(self.tx, &name)? {
            Ok(Some(entry.try_as_table_function_entry()?.function.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn require_resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>> {
        self.resolve_table_function(reference)?.ok_or_else(|| {
            RayexecError::new(format!(
                "Missing table function for reference '{}'",
                reference
            ))
        })
    }

    /// Resolve a table or cte.
    pub async fn resolve_table_or_cte(
        &self,
        reference: &ast::ObjectReference,
        bind_data: &BindData,
    ) -> Result<Option<BoundTableOrCteReference>> {
        // TODO: Seach path.
        let [catalog, schema, table] = match reference.0.len() {
            1 => {
                let name = reference.0[0].as_normalized_string();

                // Check bind data for cte that would satisfy this reference.
                if let Some(cte) = bind_data.find_cte(&name) {
                    return Ok(Some(BoundTableOrCteReference::Cte { cte_idx: cte }));
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
                return Err(RayexecError::new(
                    "Unexpected number of identifiers in table reference",
                ))
            }
        };

        let database = self.context.get_database(&catalog)?;

        // Try reading from in-memory catalog first.
        if let Some(entry) = self.resolve_from_memory_catalog(database, &schema, &table)? {
            return Ok(Some(BoundTableOrCteReference::Table {
                catalog,
                schema,
                entry,
            }));
        }

        // If we don't have it, try loading from external catalog.
        match database.catalog_storage.as_ref() {
            Some(storage) => {
                let ent = match storage.load_table(&schema, &table).await? {
                    Some(ent) => ent,
                    None => return Ok(None),
                };

                // We may need to create a schema in memory as well if we've
                // successfully loaded the table.
                let schema_ent = match database.catalog.get_schema(self.tx, &schema)? {
                    Some(schema) => schema,
                    None => database.catalog.create_schema(
                        self.tx,
                        &CreateSchemaInfo {
                            name: schema.clone(),
                            on_conflict: OnConflict::Error,
                        },
                    )?,
                };

                schema_ent.create_table(
                    self.tx,
                    &CreateTableInfo {
                        name: table.clone(),
                        columns: ent.columns,
                        on_conflict: OnConflict::Error,
                    },
                )?;
            }
            None => {
                // Nothing to load from. Return None instead of an error to the
                // remote side in hybrid execution to potentially load from
                // external source.
                return Ok(None);
            }
        }

        // Read from catalog again.
        if let Some(entry) = self.resolve_from_memory_catalog(database, &schema, &table)? {
            Ok(Some(BoundTableOrCteReference::Table {
                catalog,
                schema,
                entry,
            }))
        } else {
            Ok(None)
        }
    }

    fn resolve_from_memory_catalog(
        &self,
        database: &Database,
        schema: &str,
        table: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let schema_ent = match database.catalog.get_schema(self.tx, schema)? {
            Some(ent) => ent,
            None => return Ok(None),
        };

        schema_ent.get_table(self.tx, table)
    }

    pub async fn require_resolve_table_or_cte(
        &self,
        reference: &ast::ObjectReference,
        bind_data: &BindData,
    ) -> Result<BoundTableOrCteReference> {
        self.resolve_table_or_cte(reference, bind_data)
            .await?
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "Missing table or view for reference '{}'",
                    reference
                ))
            })
    }
}
