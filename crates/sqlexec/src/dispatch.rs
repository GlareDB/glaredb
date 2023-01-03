//! Adapter types for dispatching to table sources.
use crate::catalog::access::AccessMethod;
use crate::catalog::constants::INTERNAL_SCHEMA;
use crate::catalog::errors::{CatalogError, Result};
use crate::catalog::system::{
    columns_memory_table, schemas_memory_table, tables_memory_table, views_memory_table,
};
use crate::catalog::transaction::StubCatalogContext;
use crate::context::SessionContext;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datasource_bigquery::BigQueryAccessor;
use datasource_postgres::PostgresAccessor;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;

/// Dispatch to the appropriate table sources.
pub struct SessionDispatcher<'a> {
    ctx: &'a SessionContext,
}

impl<'a> SessionDispatcher<'a> {
    /// Create a new dispatcher.
    pub fn new(ctx: &'a SessionContext) -> Self {
        SessionDispatcher { ctx }
    }

    /// Return a datafusion table provider based on how the table should be
    /// accessed.
    ///
    /// Returns `None` if no table or view exist within a schema.
    pub fn dispatch_access(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let catalog = self.ctx.get_catalog();
        let schema_ent = catalog
            .schemas
            .get_entry(&StubCatalogContext, schema)
            .ok_or_else(|| CatalogError::MissingEntry {
                typ: "schema",
                name: schema.to_string(),
            })?;

        // Check table entries first.
        if let Some(table) = schema_ent.tables.get_entry(&StubCatalogContext, name) {
            return match &table.access {
                AccessMethod::InternalMemory => {
                    // Just match on the built-in tables.
                    match (table.schema.as_str(), table.name.as_str()) {
                        (INTERNAL_SCHEMA, "views") => Ok(Some(Arc::new(views_memory_table(
                            &StubCatalogContext,
                            catalog,
                        )))),
                        (INTERNAL_SCHEMA, "schemas") => Ok(Some(Arc::new(schemas_memory_table(
                            &StubCatalogContext,
                            catalog,
                        )))),
                        (INTERNAL_SCHEMA, "tables") => Ok(Some(Arc::new(tables_memory_table(
                            &StubCatalogContext,
                            catalog,
                        )))),
                        (INTERNAL_SCHEMA, "columns") => Ok(Some(Arc::new(columns_memory_table(
                            &StubCatalogContext,
                            catalog,
                        )))),
                        (schema, name) => Err(CatalogError::MissingTableForAccessMethod {
                            method: AccessMethod::InternalMemory,
                            schema: schema.to_string(),
                            name: name.to_string(),
                        }),
                    }
                }
                AccessMethod::Postgres(pg) => {
                    // TODO: We'll probably want an "external dispatcher" of sorts.
                    // TODO: Try not to block on.
                    let pg = pg.clone();
                    let result: Result<_, datasource_postgres::errors::PostgresError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = PostgresAccessor::connect(pg).await?;
                                let provider = accessor.into_table_provider(true).await?;
                                Ok(provider)
                            })
                        });
                    let provider = result?;
                    Ok(Some(Arc::new(provider)))
                }
                AccessMethod::BigQuery(bq) => {
                    let bq = bq.clone();
                    let result: Result<_, datasource_bigquery::errors::BigQueryError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = BigQueryAccessor::connect(bq).await?;
                                let provider = accessor.into_table_provider(true).await?;
                                Ok(provider)
                            })
                        });
                    let provider = result?;
                    Ok(Some(Arc::new(provider)))
                }
                other => Err(CatalogError::UnhandleableAccess(other.clone())),
            };
        }

        // Fall back to checking views.
        if let Some(view) = schema_ent.views.get_entry(&StubCatalogContext, name) {
            // Do some late planning.
            let plan = self
                .ctx
                .late_view_plan(&view.sql)
                .map_err(|e| CatalogError::LatePlanning(e.to_string()))?;

            return Ok(Some(Arc::new(ViewTable::try_new(plan, None)?)));
        }

        Ok(None)
    }
}
