//! Adapter types for dispatching to table sources.
use crate::catalog::access::AccessMethod;
use crate::catalog::builtins::{GLARE_COLUMNS, GLARE_SCHEMAS, GLARE_TABLES, GLARE_VIEWS};
use crate::catalog::errors::{CatalogError, Result};
use crate::catalog::transaction::{CatalogContext, StubCatalogContext};
use crate::catalog::Catalog;
use crate::context::SessionContext;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datasource_bigquery::BigQueryAccessor;
use datasource_object_store::gcs::GcsAccessor;
use datasource_object_store::local::LocalAccessor;
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
                AccessMethod::System => {
                    let table = SystemTableDispatcher::new(&StubCatalogContext, catalog)
                        .dispatch(schema, name)?;
                    Ok(Some(table))
                }
                AccessMethod::Postgres(pg) => {
                    // TODO: We'll probably want an "external dispatcher" of sorts.
                    // TODO: Try not to block on.
                    let pg = pg.clone();
                    let predicate_pushdown = *self
                        .ctx
                        .get_session_vars()
                        .postgres_predicate_pushdown
                        .value();
                    let result: Result<_, datasource_postgres::errors::PostgresError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = PostgresAccessor::connect(pg).await?;
                                let provider =
                                    accessor.into_table_provider(predicate_pushdown).await?;
                                Ok(provider)
                            })
                        });
                    let provider = result?;
                    Ok(Some(Arc::new(provider)))
                }
                AccessMethod::BigQuery(bq) => {
                    let bq = bq.clone();
                    let predicate_pushdown = *self
                        .ctx
                        .get_session_vars()
                        .bigquery_predicate_pushdown
                        .value();
                    let result: Result<_, datasource_bigquery::errors::BigQueryError> =
                        task::block_in_place(move || {
                            Handle::current().block_on(async move {
                                let accessor = BigQueryAccessor::connect(bq).await?;
                                let provider =
                                    accessor.into_table_provider(predicate_pushdown).await?;
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
                                let accessor = LocalAccessor::new(local).await?;
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
                                let accessor = GcsAccessor::new(gcs).await?;
                                let provider = accessor.into_table_provider(true).await?;
                                Ok(provider)
                            })
                        });
                    let provider = result?;
                    Ok(Some(Arc::new(provider)))
                }
                AccessMethod::Debug(debug) => Ok(Some(debug.clone().into_table_provider())),
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

/// Dispatch to builtin system tables.
struct SystemTableDispatcher<'a, C> {
    ctx: &'a C,
    catalog: &'a Catalog,
}

impl<'a, C: CatalogContext> SystemTableDispatcher<'a, C> {
    fn new(ctx: &'a C, catalog: &'a Catalog) -> Self {
        SystemTableDispatcher { ctx, catalog }
    }

    fn dispatch(&self, schema: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        Ok(if GLARE_TABLES.matches(schema, name) {
            Arc::new(self.build_glare_tables())
        } else if GLARE_COLUMNS.matches(schema, name) {
            Arc::new(self.build_glare_columns())
        } else if GLARE_VIEWS.matches(schema, name) {
            Arc::new(self.build_glare_views())
        } else if GLARE_SCHEMAS.matches(schema, name) {
            Arc::new(self.build_glare_schemas())
        } else {
            return Err(CatalogError::MissingTableForAccessMethod {
                method: AccessMethod::System,
                schema: schema.to_string(),
                name: name.to_string(),
            });
        })
    }

    fn build_glare_schemas(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SCHEMAS.arrow_schema());
        let mut schema_names = StringBuilder::new();
        for schema in self.catalog.schemas.iter(self.ctx) {
            schema_names.append_value(&schema.name);
        }
        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(schema_names.finish())])
                .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_tables(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TABLES.arrow_schema());

        let mut schema_names = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut column_counts = UInt32Builder::new();
        let mut accesses = StringBuilder::new();

        for schema in self.catalog.schemas.iter(self.ctx) {
            for table in schema.tables.iter(self.ctx) {
                schema_names.append_value(&table.schema);
                table_names.append_value(&table.name);
                column_counts.append_value(table.columns.len() as u32);
                accesses.append_value(table.access.to_string());
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(column_counts.finish()),
                Arc::new(accesses.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_columns(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_COLUMNS.arrow_schema());

        let mut schema_names = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut column_names = StringBuilder::new();
        let mut column_indexes = UInt32Builder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = BooleanBuilder::new();

        for schema in self.catalog.schemas.iter(self.ctx) {
            for table in schema.tables.iter(self.ctx) {
                for (i, col) in table.columns.iter().enumerate() {
                    schema_names.append_value(&table.schema);
                    table_names.append_value(&table.name);
                    column_names.append_value(&col.name);
                    column_indexes.append_value(i as u32);
                    data_types.append_value(col.datatype.to_string());
                    is_nullables.append_value(col.nullable);
                }
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(column_names.finish()),
                Arc::new(column_indexes.finish()),
                Arc::new(data_types.finish()),
                Arc::new(is_nullables.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_views(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_VIEWS.arrow_schema());

        let mut schema_names = StringBuilder::new();
        let mut view_names = StringBuilder::new();
        let mut sqls = StringBuilder::new();

        for schema in self.catalog.schemas.iter(self.ctx) {
            for view in schema.views.iter(self.ctx) {
                schema_names.append_value(&view.schema);
                view_names.append_value(&view.name);
                sqls.append_value(&view.sql);
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(schema_names.finish()),
                Arc::new(view_names.finish()),
                Arc::new(sqls.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }
}
