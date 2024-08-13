use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    functions::table::TableFunction,
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{bound_table::BoundTableOrCteReference, BindData};

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

        if let Some(entry) = self
            .context
            .get_catalog(&catalog)?
            .get_table_fn(self.tx, &schema, &name)?
        {
            Ok(Some(entry))
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

        if let Some(entry) = self
            .context
            .get_catalog(&catalog)?
            .get_table_entry(self.tx, &schema, &table)
            .await?
        {
            Ok(Some(BoundTableOrCteReference::Table {
                catalog,
                schema,
                entry,
            }))
        } else {
            Ok(None)
        }
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
