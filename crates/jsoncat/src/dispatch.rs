//! Adapter types for dispatching to table sources.
use crate::access::AccessMethod;
use crate::catalog::Catalog;
use crate::constants::INTERNAL_SCHEMA;
use crate::errors::{CatalogError, Result};
use crate::system::{
    columns_memory_table, schemas_memory_table, tables_memory_table, views_memory_table,
};
use crate::transaction::Context;
use access::external::postgres::PostgresAccessor;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::LogicalPlan;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;

/// Plan a single sql statement, generating a DataFusion logical plan.
///
/// Some objects, namely views, require that we generate logical plans very late
/// in the planning process.
pub trait LatePlanner {
    type Error: std::fmt::Display;
    fn plan_sql(&self, sql: &str) -> std::result::Result<LogicalPlan, Self::Error>;
}

/// Dispatch to the appropriate table sources.
pub struct CatalogDispatcher<'a, C> {
    catalog: &'a Arc<Catalog>,
    ctx: &'a C,
}

impl<'a, C: Context> CatalogDispatcher<'a, C> {
    /// Create a new dispatcher.
    pub fn new(ctx: &'a C, catalog: &'a Arc<Catalog>) -> Self {
        CatalogDispatcher { catalog, ctx }
    }

    /// Return a datafusion table provider based on how the table should be
    /// accessed.
    ///
    /// Returns `None` if no table or view exist within a schema.
    ///
    /// Note `planner` is only used when accessing views.
    pub fn dispatch_access<P: LatePlanner>(
        &self,
        planner: &P,
        schema: &str,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let schema_ent = self
            .catalog
            .schemas
            .get_entry(self.ctx, schema)
            .ok_or_else(|| CatalogError::MissingEntry {
                typ: "schema",
                name: schema.to_string(),
            })?;

        // Check table entries first.
        if let Some(table) = schema_ent.tables.get_entry(self.ctx, name) {
            return match &table.access {
                AccessMethod::InternalMemory => {
                    // Just match on the built-in tables.
                    match (table.schema.as_str(), table.name.as_str()) {
                        (INTERNAL_SCHEMA, "views") => {
                            Ok(Some(Arc::new(views_memory_table(self.ctx, self.catalog))))
                        }
                        (INTERNAL_SCHEMA, "schemas") => {
                            Ok(Some(Arc::new(schemas_memory_table(self.ctx, self.catalog))))
                        }
                        (INTERNAL_SCHEMA, "tables") => {
                            Ok(Some(Arc::new(tables_memory_table(self.ctx, self.catalog))))
                        }
                        (INTERNAL_SCHEMA, "columns") => {
                            Ok(Some(Arc::new(columns_memory_table(self.ctx, self.catalog))))
                        }
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
                    let result: Result<_, access::errors::AccessError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = PostgresAccessor::connect(pg.clone()).await?;
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
        if let Some(view) = schema_ent.views.get_entry(self.ctx, name) {
            // Do some late planning.
            let plan = planner
                .plan_sql(&view.sql)
                .map_err(|e| CatalogError::LatePlanning(e.to_string()))?;

            return Ok(Some(Arc::new(ViewTable::try_new(plan, None)?)));
        }

        Ok(None)
    }
}
