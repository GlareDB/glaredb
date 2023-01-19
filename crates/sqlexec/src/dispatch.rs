//! Adapter types for dispatching to table sources.
use crate::context::SessionContext;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use metastore::builtins::{
    GLARE_COLUMNS, GLARE_CONNECTIONS, GLARE_SCHEMAS, GLARE_TABLES, GLARE_VIEWS,
};
use metastore::session::SessionCatalog;
use metastore::types::catalog::{CatalogEntry, EntryType, ExternalTableEntry, ViewEntry};
use std::sync::Arc;
//use tokio::runtime::Handle;
//use tokio::task;
//use tracing::trace;
//use datasource_bigquery::{BigQueryAccessor, BigQueryTableAccess};
//use datasource_object_store::gcs::{GcsAccessor, GcsTableAccess};
//use datasource_object_store::local::{LocalAccessor, LocalTableAccess};
//use datasource_object_store::s3::{S3Accessor, S3TableAccess};
//use datasource_postgres::{PostgresAccessor, PostgresTableAccess};

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Missing catalog entry; schema: {schema}, name: {name}")]
    MissingEntry { schema: String, name: String },

    #[error("Missing builtin table; schema: {schema}, name: {name}")]
    MissingBuiltinTable { schema: String, name: String },

    #[error("Invalid entry for table dispatch: {0}")]
    InvalidEntryTypeForDispatch(metastore::types::catalog::EntryType),

    #[error("Unhandled entry for table dispatch: {0}")]
    UnhandledEntryType(metastore::types::catalog::EntryType),

    #[error("failed to do late planning: {0}")]
    LatePlanning(Box<crate::errors::ExecError>),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
}

type Result<T, E = DispatchError> = std::result::Result<T, E>;

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
    pub fn dispatch_access(&self, schema: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        let catalog = self.ctx.get_session_catalog();

        let ent =
            catalog
                .resolve_entry(schema, name)
                .ok_or_else(|| DispatchError::MissingEntry {
                    schema: schema.to_string(),
                    name: name.to_string(),
                })?;

        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(
            ent.entry_type(),
            EntryType::View | EntryType::Table | EntryType::ExternalTable
        ) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        // Dispatch to system tables if builtin...
        if ent.get_meta().builtin && ent.entry_type() == EntryType::Table {
            let table = SystemTableDispatcher::new(catalog).dispatch(schema, name)?;
            return Ok(table);
        }

        match ent {
            CatalogEntry::View(view) => self.dispatch_view(&view),
            CatalogEntry::ExternalTable(table) => self.dispatch_external_table(&table),
            // Note that all 'table' entries should have already been handled
            // with the above builtin table dispatcher.
            other => Err(DispatchError::UnhandledEntryType(other.entry_type())),
        }
    }

    fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .ctx
            .late_view_plan(&view.sql)
            .map_err(|e| DispatchError::LatePlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }

    fn dispatch_external_table(
        &self,
        _table: &ExternalTableEntry,
    ) -> Result<Arc<dyn TableProvider>> {
        //     return match &table.access {
        //         AccessOrConnection::Access(AccessMethod::System) => {
        //             let table = SystemTableDispatcher::new(&StubCatalogContext, sess_catalog)
        //                 .dispatch(schema, name)?;
        //             Ok(Some(table))
        //         }
        //         AccessOrConnection::Access(AccessMethod::Postgres(pg)) => {
        //             // TODO: We'll probably want an "external dispatcher" of sorts.
        //             // TODO: Try not to block on.
        //             let pg = pg.clone();
        //             let predicate_pushdown = *self
        //                 .ctx
        //                 .get_session_vars()
        //                 .postgres_predicate_pushdown
        //                 .value();
        //             let result: Result<_, datasource_postgres::errors::PostgresError> =
        //                 task::block_in_place(move || {
        //                     Handle::current().block_on(async move {
        //                         let accessor = PostgresAccessor::connect(pg).await?;
        //                         let provider =
        //                             accessor.into_table_provider(predicate_pushdown).await?;
        //                         Ok(provider)
        //                     })
        //                 });
        //             let provider = result?;
        //             Ok(Some(Arc::new(provider)))
        //         }
        //         AccessOrConnection::Access(AccessMethod::BigQuery(bq)) => {
        //             let bq = bq.clone();
        //             let predicate_pushdown = *self
        //                 .ctx
        //                 .get_session_vars()
        //                 .bigquery_predicate_pushdown
        //                 .value();
        //             let result: Result<_, datasource_bigquery::errors::BigQueryError> =
        //                 task::block_in_place(move || {
        //                     Handle::current().block_on(async move {
        //                         let accessor = BigQueryAccessor::connect(bq).await?;
        //                         let provider =
        //                             accessor.into_table_provider(predicate_pushdown).await?;
        //                         Ok(provider)
        //                     })
        //                 });
        //             let provider = result?;
        //             Ok(Some(Arc::new(provider)))
        //         }
        //         AccessOrConnection::Access(AccessMethod::Local(local)) => {
        //             trace!("Using Local filesystem to access parquet file");
        //             let local = local.clone();
        //             let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
        //                 task::block_in_place(move || {
        //                     Handle::current().block_on(async move {
        //                         let accessor = LocalAccessor::new(local).await?;
        //                         let provider = accessor.into_table_provider(true).await?;
        //                         Ok(provider)
        //                     })
        //                 });
        //             let provider = result?;
        //             Ok(Some(Arc::new(provider)))
        //         }
        //         AccessOrConnection::Access(AccessMethod::S3(s3)) => {
        //             trace!("Using S3 to access parquet file");
        //             let s3 = s3.clone();
        //             let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
        //                 task::block_in_place(move || {
        //                     Handle::current().block_on(async move {
        //                         let accessor = S3Accessor::new(s3).await?;
        //                         let provider = accessor.into_table_provider(true).await?;
        //                         Ok(provider)
        //                     })
        //                 });
        //             let provider = result?;
        //             Ok(Some(Arc::new(provider)))
        //         }
        //         AccessOrConnection::Access(AccessMethod::Gcs(gcs)) => {
        //             trace!("Using GCS to access parquet file");
        //             let gcs = gcs.clone();
        //             let result: Result<_, datasource_object_store::errors::ObjectStoreSourceError> =
        //                 task::block_in_place(move || {
        //                     Handle::current().block_on(async move {
        //                         let accessor = GcsAccessor::new(gcs).await?;
        //                         let provider = accessor.into_table_provider(true).await?;
        //                         Ok(provider)
        //                     })
        //                 });
        //             let provider = result?;
        //             Ok(Some(Arc::new(provider)))
        //         }
        //         AccessOrConnection::Access(AccessMethod::Debug(debug)) => {
        //             Ok(Some(debug.clone().into_table_provider()))
        //         }
        //         AccessOrConnection::Connection(name) => {
        //             let conn = self
        //                 .ctx
        //                 .get_connection(name)
        //                 .map_err(|e| internal!("{}", e))?;
        //             match (&table.table_options, conn.as_ref()) {
        //                 (
        //                     TableOptions::Debug { typ },
        //                     ConnectionEntry {
        //                         method: ConnectionMethod::Debug,
        //                         ..
        //                     },
        //                 ) => Ok(Some(typ.clone().into_table_provider())),
        //                 (
        //                     TableOptions::Postgres { schema, table },
        //                     ConnectionEntry {
        //                         method: ConnectionMethod::Postgres { connection_string },
        //                         ..
        //                     },
        //                 ) => {
        //                     let table_access = PostgresTableAccess {
        //                         schema: schema.clone(),
        //                         name: table.clone(),
        //                         connection_string: connection_string.clone(),
        //                     };
        //                     let predicate_pushdown = *self
        //                         .ctx
        //                         .get_session_vars()
        //                         .postgres_predicate_pushdown
        //                         .value();
        //                     let result: Result<_, datasource_postgres::errors::PostgresError> =
        //                         task::block_in_place(move || {
        //                             Handle::current().block_on(async move {
        //                                 let accessor =
        //                                     PostgresAccessor::connect(table_access).await?;
        //                                 let provider = accessor
        //                                     .into_table_provider(predicate_pushdown)
        //                                     .await?;
        //                                 Ok(provider)
        //                             })
        //                         });
        //                     let provider = result?;
        //                     Ok(Some(Arc::new(provider)))
        //                 }
        //                 (
        //                     TableOptions::BigQuery {
        //                         dataset_id,
        //                         table_id,
        //                     },
        //                     ConnectionEntry {
        //                         method:
        //                             ConnectionMethod::BigQuery {
        //                                 service_account_key,
        //                                 project_id,
        //                             },
        //                         ..
        //                     },
        //                 ) => {
        //                     let table_access = BigQueryTableAccess {
        //                         gcp_service_acccount_key_json: service_account_key.clone(),
        //                         gcp_project_id: project_id.clone(),
        //                         dataset_id: dataset_id.clone(),
        //                         table_id: table_id.clone(),
        //                     };
        //                     let predicate_pushdown = *self
        //                         .ctx
        //                         .get_session_vars()
        //                         .bigquery_predicate_pushdown
        //                         .value();
        //                     let result: Result<_, datasource_bigquery::errors::BigQueryError> =
        //                         task::block_in_place(move || {
        //                             Handle::current().block_on(async move {
        //                                 let accessor =
        //                                     BigQueryAccessor::connect(table_access).await?;
        //                                 let provider = accessor
        //                                     .into_table_provider(predicate_pushdown)
        //                                     .await?;
        //                                 Ok(provider)
        //                             })
        //                         });
        //                     let provider = result?;
        //                     Ok(Some(Arc::new(provider)))
        //                 }
        //                 (
        //                     TableOptions::Local { location },
        //                     ConnectionEntry {
        //                         method: ConnectionMethod::Local,
        //                         ..
        //                     },
        //                 ) => {
        //                     let table_access = LocalTableAccess {
        //                         location: location.clone(),
        //                     };
        //                     let result: Result<
        //                         _,
        //                         datasource_object_store::errors::ObjectStoreSourceError,
        //                     > = task::block_in_place(move || {
        //                         Handle::current().block_on(async move {
        //                             let accessor = LocalAccessor::new(table_access).await?;
        //                             let provider = accessor.into_table_provider(true).await?;
        //                             Ok(provider)
        //                         })
        //                     });
        //                     let provider = result?;
        //                     Ok(Some(Arc::new(provider)))
        //                 }
        //                 (
        //                     TableOptions::Gcs {
        //                         bucket_name,
        //                         location,
        //                     },
        //                     ConnectionEntry {
        //                         method:
        //                             ConnectionMethod::Gcs {
        //                                 service_account_key,
        //                             },
        //                         ..
        //                     },
        //                 ) => {
        //                     let table_access = GcsTableAccess {
        //                         service_acccount_key_json: service_account_key.clone(),
        //                         bucket_name: bucket_name.clone(),
        //                         location: location.clone(),
        //                     };
        //                     let result: Result<
        //                         _,
        //                         datasource_object_store::errors::ObjectStoreSourceError,
        //                     > = task::block_in_place(move || {
        //                         Handle::current().block_on(async move {
        //                             let accessor = GcsAccessor::new(table_access).await?;
        //                             let provider = accessor.into_table_provider(true).await?;
        //                             Ok(provider)
        //                         })
        //                     });
        //                     let provider = result?;
        //                     Ok(Some(Arc::new(provider)))
        //                 }
        //                 (
        //                     TableOptions::S3 {
        //                         region,
        //                         bucket_name,
        //                         location,
        //                     },
        //                     ConnectionEntry {
        //                         method:
        //                             ConnectionMethod::S3 {
        //                                 access_key_id,
        //                                 access_key_secret,
        //                             },
        //                         ..
        //                     },
        //                 ) => {
        //                     let table_access = S3TableAccess {
        //                         region: region.clone(),
        //                         bucket_name: bucket_name.clone(),
        //                         location: location.clone(),
        //                         access_key_id: access_key_id.clone(),
        //                         secret_access_key: access_key_secret.clone(),
        //                     };
        //                     let result: Result<
        //                         _,
        //                         datasource_object_store::errors::ObjectStoreSourceError,
        //                     > = task::block_in_place(move || {
        //                         Handle::current().block_on(async move {
        //                             let accessor = S3Accessor::new(table_access).await?;
        //                             let provider = accessor.into_table_provider(true).await?;
        //                             Ok(provider)
        //                         })
        //                     });
        //                     let provider = result?;
        //                     Ok(Some(Arc::new(provider)))
        //                 }
        //                 (opts, conn) => Err(internal!(
        //                     "unhandled types; options: {:?}, conn: {:?}",
        //                     opts,
        //                     conn
        //                 )),
        //             }
        //         }
        //         AccessOrConnection::Access(AccessMethod::Unknown) => {
        //             Err(DispatchError::UnhandleableAccess(table.access.clone()))
        //         }
        //     };
        unimplemented!()
    }
}

/// Dispatch to builtin system tables.
struct SystemTableDispatcher<'a> {
    catalog: &'a SessionCatalog,
}

impl<'a> SystemTableDispatcher<'a> {
    fn new(catalog: &'a SessionCatalog) -> Self {
        SystemTableDispatcher { catalog }
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
        } else if GLARE_CONNECTIONS.matches(schema, name) {
            Arc::new(self.build_glare_connections())
        } else {
            return Err(DispatchError::MissingBuiltinTable {
                schema: schema.to_string(),
                name: name.to_string(),
            });
        })
    }

    fn build_glare_schemas(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SCHEMAS.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();

        for schema in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Schema)
        {
            oids.append_value(schema.oid);
            builtins.append_value(schema.builtin);
            schema_names.append_value(&schema.entry.get_meta().name);
        }
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_tables(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TABLES.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();
        let mut table_names = StringBuilder::new();

        for table in self.catalog.iter_entries().filter(|ent| {
            ent.entry_type() == EntryType::Table || ent.entry_type() == EntryType::ExternalTable
        }) {
            oids.append_value(table.oid);
            builtins.append_value(table.builtin);
            schema_names.append_value(
                &table
                    .schema_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            table_names.append_value(&table.entry.get_meta().name);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_columns(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_COLUMNS.arrow_schema());

        let mut table_oids = UInt32Builder::new();
        let mut schema_names = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut column_names = StringBuilder::new();
        let mut column_indexes = UInt32Builder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = BooleanBuilder::new();

        for table in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            let ent = match table.entry {
                CatalogEntry::Table(ent) => ent,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            for (i, col) in ent.columns.iter().enumerate() {
                table_oids.append_value(table.oid);
                schema_names.append_value(
                    &table
                        .schema_entry
                        .map(|schema| schema.get_meta().name.as_str())
                        .unwrap_or("<invalid>"),
                );
                table_names.append_value(&table.entry.get_meta().name);
                column_names.append_value(&col.name);
                column_indexes.append_value(i as u32);
                data_types.append_value(col.arrow_type.to_string());
                is_nullables.append_value(col.nullable);
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(table_oids.finish()),
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

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();
        let mut view_names = StringBuilder::new();
        let mut sqls = StringBuilder::new();

        for view in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::View)
        {
            let ent = match view.entry {
                CatalogEntry::View(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oids.append_value(view.oid);
            builtins.append_value(view.builtin);
            schema_names.append_value(
                &view
                    .schema_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            view_names.append_value(&view.entry.get_meta().name);
            sqls.append_value(&ent.sql);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(view_names.finish()),
                Arc::new(sqls.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_connections(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_CONNECTIONS.arrow_schema());

        let mut oids = UInt32Builder::new();
        let mut builtins = BooleanBuilder::new();
        let mut schema_names = StringBuilder::new();
        let mut connection_names = StringBuilder::new();
        let mut connection_types = StringBuilder::new();

        for conn in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Connection)
        {
            let ent = match conn.entry {
                CatalogEntry::Connection(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oids.append_value(conn.oid);
            builtins.append_value(conn.builtin);
            schema_names.append_value(
                &conn
                    .schema_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            connection_names.append_value(&conn.entry.get_meta().name);
            connection_types.append_value(ent.options.to_string());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oids.finish()),
                Arc::new(builtins.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(connection_names.finish()),
                Arc::new(connection_types.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }
}
