//! Adapter types for dispatching to table sources.
use crate::context::SessionContext;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datasource_bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasource_mongodb::{MongoAccessInfo, MongoAccessor, MongoTableAccessInfo};
use datasource_mysql::{MysqlAccessor, MysqlTableAccess};
use datasource_object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasource_object_store::local::{LocalAccessor, LocalTableAccess};
use datasource_object_store::s3::{S3Accessor, S3TableAccess};
use datasource_postgres::{PostgresAccessor, PostgresTableAccess};
use metastore::builtins::{
    DEFAULT_CATALOG, GLARE_COLUMNS, GLARE_DATABASES, GLARE_SCHEMAS, GLARE_SESSION_QUERY_METRICS,
    GLARE_TABLES, GLARE_VIEWS,
};
use metastore::session::SessionCatalog;
use metastore::types::catalog::{
    CatalogEntry, DatabaseEntry, EntryMeta, EntryType, TableEntry, ViewEntry,
};
use metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo, DatabaseOptionsMysql,
    DatabaseOptionsPostgres, TableOptions, TableOptionsBigQuery, TableOptionsDebug,
    TableOptionsGcs, TableOptionsInternal, TableOptionsLocal, TableOptionsMongo, TableOptionsMysql,
    TableOptionsPostgres, TableOptionsS3,
};
use std::sync::Arc;
use tracing::error;

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Missing database: {database}")]
    MissingDatabase { database: String },

    #[error("Missing catalog entry; schema: {schema}, name: {name}")]
    MissingEntry { schema: String, name: String },

    #[error("Missing builtin table; schema: {schema}, name: {name}")]
    MissingBuiltinTable { schema: String, name: String },

    #[error("Missing object with oid: {0}")]
    MissingObjectWithOid(u32),

    #[error("Unable to retrieve ssh tunnel connection: {0}")]
    MissingSshTunnel(Box<crate::errors::ExecError>),

    #[error("Invalid entry for table dispatch: {0}")]
    InvalidEntryTypeForDispatch(EntryType),

    #[error("Unhandled entry for table dispatch: {0:?}")]
    UnhandledEntry(EntryMeta),

    #[error("failed to do late planning: {0}")]
    LatePlanning(Box<crate::planner::errors::PlanError>),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    DebugDatasource(#[from] datasource_debug::errors::DebugError),
    #[error(transparent)]
    PostgresDatasource(#[from] datasource_postgres::errors::PostgresError),
    #[error(transparent)]
    BigQueryDatasource(#[from] datasource_bigquery::errors::BigQueryError),
    #[error(transparent)]
    MysqlDatasource(#[from] datasource_mysql::errors::MysqlError),
    #[error(transparent)]
    ObjectStoreDatasource(#[from] datasource_object_store::errors::ObjectStoreSourceError),
    #[error(transparent)]
    MongoDatasource(#[from] datasource_mongodb::errors::MongoError),
    #[error(transparent)]
    CommonDatasource(#[from] datasource_common::errors::Error),
}

impl DispatchError {
    /// Whether or not this error should indicate to the planner to try looking
    /// in a different schema for the requested object.
    ///
    /// For example, if a user's search path is '[public, other]', and 'table_a'
    /// exists in 'other', then the dispatch will fail the first time with
    /// `MissingEntry` since it will look for 'public.table_a' first. In such
    /// cases, we should try the next schema.
    pub fn should_try_next_schema(&self) -> bool {
        matches!(
            self,
            DispatchError::MissingEntry { .. } | DispatchError::InvalidEntryTypeForDispatch(_)
        )
    }
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
    pub async fn dispatch_access(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let catalog = self.ctx.get_session_catalog();

        // "External" database.
        if database != DEFAULT_CATALOG {
            let db = match catalog.resolve_database(database) {
                Some(db) => db,
                None => {
                    return Err(DispatchError::MissingDatabase {
                        database: database.to_string(),
                    })
                }
            };
            return self.dispatch_external_database(db, schema, name).await;
        }

        let ent = catalog
            .resolve_entry(database, schema, name)
            .ok_or_else(|| DispatchError::MissingEntry {
                schema: schema.to_string(),
                name: name.to_string(),
            })?;

        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(ent.entry_type(), EntryType::View | EntryType::Table) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        match ent {
            CatalogEntry::View(view) => self.dispatch_view(view).await,
            // Dispatch to builtin tables.
            CatalogEntry::Table(tbl) if tbl.meta.builtin => {
                SystemTableDispatcher::new(self.ctx).dispatch(schema, name)
            }
            // Dispatch to external tables.
            CatalogEntry::Table(tbl) if tbl.meta.external => {
                self.dispatch_external_table(tbl).await
            }
            // We don't currently support non-external, non-builtin tables.
            other => Err(DispatchError::UnhandledEntry(other.get_meta().clone())),
        }
    }

    async fn dispatch_external_database(
        &self,
        db: &DatabaseEntry,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        match &db.options {
            DatabaseOptions::Internal(_) => unimplemented!(),
            DatabaseOptions::Debug(_) => unimplemented!(),
            DatabaseOptions::Postgres(DatabaseOptionsPostgres { connection_string }) => {
                let table_access = PostgresTableAccess {
                    schema: schema.to_string(),
                    name: name.to_string(),
                    connection_string: connection_string.clone(),
                };

                let accessor = PostgresAccessor::connect(table_access, None).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::BigQuery(DatabaseOptionsBigQuery {
                service_account_key,
                project_id,
            }) => {
                let table_access = BigQueryTableAccess {
                    gcp_service_acccount_key_json: service_account_key.clone(),
                    gcp_project_id: project_id.clone(),
                    dataset_id: schema.to_string(),
                    table_id: name.to_string(),
                };

                let accessor = BigQueryAccessor::connect(table_access).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::Mysql(DatabaseOptionsMysql { connection_string }) => {
                let table_access = MysqlTableAccess {
                    schema: schema.to_string(),
                    name: name.to_string(),
                    connection_string: connection_string.clone(),
                };

                let accessor = MysqlAccessor::connect(table_access, None).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::Mongo(DatabaseOptionsMongo { connection_string }) => {
                let access_info = MongoAccessInfo {
                    connection_string: connection_string.clone(),
                };
                let table_info = MongoTableAccessInfo {
                    database: schema.to_string(), // A mongodb database is pretty much a schema.
                    collection: name.to_string(),
                };
                let accessor = MongoAccessor::connect(access_info).await?;
                let table_accessor = accessor.into_table_accessor(table_info);
                let provider = table_accessor.into_table_provider().await?;
                Ok(Arc::new(provider))
            }
        }
    }

    async fn dispatch_external_table(&self, table: &TableEntry) -> Result<Arc<dyn TableProvider>> {
        match &table.options {
            TableOptions::Internal(TableOptionsInternal {}) => unimplemented!(),
            TableOptions::Debug(TableOptionsDebug {}) => unimplemented!(),
            TableOptions::Postgres(TableOptionsPostgres {
                connection_string,
                schema,
                table,
            }) => {
                let table_access = PostgresTableAccess {
                    schema: schema.clone(),
                    name: table.clone(),
                    connection_string: connection_string.clone(),
                };

                let accessor = PostgresAccessor::connect(table_access, None).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(Arc::new(provider))
            }
            TableOptions::BigQuery(TableOptionsBigQuery {
                service_account_key,
                project_id,
                dataset_id,
                table_id,
            }) => {
                let table_access = BigQueryTableAccess {
                    gcp_service_acccount_key_json: service_account_key.to_string(),
                    gcp_project_id: project_id.to_string(),
                    dataset_id: dataset_id.to_string(),
                    table_id: table_id.to_string(),
                };

                let accessor = BigQueryAccessor::connect(table_access).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(Arc::new(provider))
            }
            TableOptions::Mysql(TableOptionsMysql {
                connection_string,
                schema,
                table,
            }) => {
                let table_access = MysqlTableAccess {
                    schema: schema.clone(),
                    name: table.clone(),
                    connection_string: connection_string.clone(),
                };

                let accessor = MysqlAccessor::connect(table_access, None).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(Arc::new(provider))
            }
            TableOptions::Mongo(TableOptionsMongo {
                connection_string,
                database,
                collection,
            }) => {
                let access_info = MongoAccessInfo {
                    connection_string: connection_string.clone(),
                };
                let table_info = MongoTableAccessInfo {
                    database: database.to_string(),
                    collection: collection.to_string(),
                };
                let accessor = MongoAccessor::connect(access_info).await?;
                let table_accessor = accessor.into_table_accessor(table_info);
                let provider = table_accessor.into_table_provider().await?;
                Ok(Arc::new(provider))
            }
            TableOptions::Local(TableOptionsLocal { location }) => {
                let table_access = LocalTableAccess {
                    location: location.clone(),
                    file_type: None,
                };
                let accessor = LocalAccessor::new(table_access).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(provider)
            }
            TableOptions::Gcs(TableOptionsGcs {
                service_account_key,
                bucket,
                location,
            }) => {
                let table_access = GcsTableAccess {
                    service_acccount_key_json: service_account_key.clone(),
                    bucket_name: bucket.clone(),
                    location: location.clone(),
                    file_type: None,
                };
                let accessor = GcsAccessor::new(table_access).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(provider)
            }
            TableOptions::S3(TableOptionsS3 {
                access_key_id,
                secret_access_key,
                region,
                bucket,
                location,
            }) => {
                let table_access = S3TableAccess {
                    region: region.clone(),
                    bucket_name: bucket.clone(),
                    location: location.clone(),
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    file_type: None,
                };
                let accessor = S3Accessor::new(table_access).await?;
                let provider = accessor.into_table_provider(true).await?;
                Ok(provider)
            }
        }
    }

    async fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .ctx
            .late_view_plan(&view.sql)
            .await
            .map_err(|e| DispatchError::LatePlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }
}

/// Dispatch to builtin system tables.
struct SystemTableDispatcher<'a> {
    ctx: &'a SessionContext,
}

impl<'a> SystemTableDispatcher<'a> {
    fn new(ctx: &'a SessionContext) -> Self {
        SystemTableDispatcher { ctx }
    }

    fn catalog(&self) -> &SessionCatalog {
        self.ctx.get_session_catalog()
    }

    fn dispatch(&self, schema: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        Ok(if GLARE_DATABASES.matches(schema, name) {
            Arc::new(self.build_glare_databases())
        } else if GLARE_TABLES.matches(schema, name) {
            Arc::new(self.build_glare_tables())
        } else if GLARE_COLUMNS.matches(schema, name) {
            Arc::new(self.build_glare_columns())
        } else if GLARE_VIEWS.matches(schema, name) {
            Arc::new(self.build_glare_views())
        } else if GLARE_SCHEMAS.matches(schema, name) {
            Arc::new(self.build_glare_schemas())
        } else if GLARE_SESSION_QUERY_METRICS.matches(schema, name) {
            Arc::new(self.build_session_query_metrics())
        } else {
            return Err(DispatchError::MissingBuiltinTable {
                schema: schema.to_string(),
                name: name.to_string(),
            });
        })
    }

    fn build_glare_databases(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_DATABASES.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();

        for db in self
            .catalog()
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Database)
        {
            oid.append_value(db.oid);
            database_name.append_value(&db.entry.get_meta().name);
            builtin.append_value(db.builtin);
            external.append_value(db.entry.get_meta().external);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(external.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_schemas(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SCHEMAS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut schema_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();

        for schema in self
            .catalog()
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Schema)
        {
            oid.append_value(schema.oid);
            database_oid.append_value(schema.entry.get_meta().parent);
            database_name.append_value(
                schema
                    .parent_entry
                    .map(|db| db.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            schema_name.append_value(&schema.entry.get_meta().name);
            builtin.append_value(schema.builtin);
        }
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_oid.finish()),
                Arc::new(database_name.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(builtin.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_tables(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TABLES.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut table_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();

        for table in self
            .catalog()
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            oid.append_value(table.oid);
            schema_oid.append_value(table.entry.get_meta().parent);
            database_oid.append_value(
                table
                    .parent_entry
                    .map(|schema| schema.get_meta().parent)
                    .unwrap_or_default(),
            );
            schema_name.append_value(
                table
                    .parent_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            table_name.append_value(&table.entry.get_meta().name);
            builtin.append_value(table.builtin);
            external.append_value(table.entry.get_meta().external);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_oid.finish()),
                Arc::new(schema_oid.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(table_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(external.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_columns(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_COLUMNS.arrow_schema());

        let mut schema_oid = UInt32Builder::new();
        let mut table_oid = UInt32Builder::new();
        let mut table_name = StringBuilder::new();
        let mut column_name = StringBuilder::new();
        let mut column_ordinal = UInt32Builder::new();
        let mut data_type = StringBuilder::new();
        let mut is_nullable = BooleanBuilder::new();

        for table in self
            .catalog()
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            let ent = match table.entry {
                CatalogEntry::Table(ent) => ent,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            for (i, col) in ent.columns.iter().enumerate() {
                schema_oid.append_value(
                    table
                        .parent_entry
                        .map(|ent| ent.get_meta().id)
                        .unwrap_or_default(),
                );
                table_oid.append_value(table.oid);
                table_name.append_value(&table.entry.get_meta().name);
                column_name.append_value(&col.name);
                column_ordinal.append_value(i as u32);
                data_type.append_value(col.arrow_type.to_string());
                is_nullable.append_value(col.nullable);
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(schema_oid.finish()),
                Arc::new(table_oid.finish()),
                Arc::new(table_name.finish()),
                Arc::new(column_name.finish()),
                Arc::new(column_ordinal.finish()),
                Arc::new(data_type.finish()),
                Arc::new(is_nullable.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_views(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_VIEWS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut view_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut sql = StringBuilder::new();

        for view in self
            .catalog()
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::View)
        {
            let ent = match view.entry {
                CatalogEntry::View(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oid.append_value(view.oid);
            database_oid.append_value(
                view.parent_entry
                    .map(|schema| schema.get_meta().parent)
                    .unwrap_or_default(),
            );
            schema_oid.append_value(view.entry.get_meta().parent);
            schema_name.append_value(
                view.parent_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            view_name.append_value(&view.entry.get_meta().name);
            builtin.append_value(view.builtin);
            sql.append_value(&ent.sql);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_oid.finish()),
                Arc::new(schema_oid.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(view_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(sql.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_session_query_metrics(&self) -> MemTable {
        let num_metrics = self.ctx.get_metrics().num_metrics();

        let mut query_text = StringBuilder::with_capacity(num_metrics, 20);
        let mut result_type = StringBuilder::with_capacity(num_metrics, 10);
        let mut execution_status = StringBuilder::with_capacity(num_metrics, 10);
        let mut error_message = StringBuilder::with_capacity(num_metrics, 20);
        let mut elapsed_compute_ns = UInt64Builder::with_capacity(num_metrics);
        let mut output_rows = UInt64Builder::with_capacity(num_metrics);

        for m in self.ctx.get_metrics().iter() {
            query_text.append_value(&m.query_text);
            result_type.append_value(m.result_type);
            execution_status.append_value(m.execution_status.as_str());
            error_message.append_option(m.error_message.as_ref());
            elapsed_compute_ns.append_option(m.elapsed_compute_ns);
            output_rows.append_option(m.output_rows);
        }

        let arrow_schema = Arc::new(GLARE_SESSION_QUERY_METRICS.arrow_schema());
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(query_text.finish()),
                Arc::new(result_type.finish()),
                Arc::new(execution_status.finish()),
                Arc::new(error_message.finish()),
                Arc::new(elapsed_compute_ns.finish()),
                Arc::new(output_rows.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }
}
