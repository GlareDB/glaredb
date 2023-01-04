//! Adapter types for dispatching to table sources.
use crate::catalog::access::AccessMethod;
use crate::catalog::constants::INTERNAL_SCHEMA;
use crate::catalog::errors::{CatalogError, Result};
use crate::catalog::system::{
    columns_memory_table, schemas_memory_table, tables_memory_table, views_memory_table,
};
use crate::catalog::transaction::Context;
use crate::catalog::Catalog;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::LogicalPlan;
use datasource_bigquery::BigQueryAccessor;
use datasource_object_store::gcs::GcsAccessor;
use datasource_object_store::local::LocalAccessor;
use datasource_postgres::PostgresAccessor;
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
                    let result: Result<_, datasource_postgres::errors::PostgresError> =
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
                AccessMethod::BigQuery(bq) => {
                    let bq = bq.clone();
                    let result: Result<_, datasource_bigquery::errors::BigQueryError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = BigQueryAccessor::connect(bq.clone()).await?;
                                let provider = accessor.into_table_provider(true).await?;
                                Ok(provider)
                            })
                        });
                    let provider = result?;
                    Ok(Some(Arc::new(provider)))
                }
                AccessMethod::Local(local) => {
                    tracing::trace!("Using Local filesystem to access parquet file");
                    let local = local.clone();
                    let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = LocalAccessor::new(local.clone()).await?;
                                let provider = accessor.into_table_provider(true).await?;
                                Ok(provider)
                            })
                        });
                    let provider = result?;
                    Ok(Some(Arc::new(provider)))
                }
                AccessMethod::Gcs(gcs) => {
                    tracing::trace!("Using GCS to access parquet file");
                    let gcs = gcs.clone();
                    let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = GcsAccessor::new(gcs.clone()).await?;
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
