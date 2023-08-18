//! Adapter types for dispatching to table sources.
use crate::context::SessionContext;
use crate::metastore::catalog::{AsyncSessionCatalog, SessionCatalog};
use datafusion::arrow::array::{
    BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::common::ssh::{key::SshKey, SshConnectionParameters};
use datasources::debug::DebugTableType;
use datasources::lake::delta::access::DeltaLakeAccessor;
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::{ObjStoreAccess, ObjStoreAccessor};
use datasources::postgres::{PostgresAccess, PostgresTableProvider};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use protogen::metastore::types::catalog::{
    CatalogEntry, DatabaseEntry, EntryMeta, EntryType, TableEntry, ViewEntry,
};
use protogen::metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsDebug, DatabaseOptionsDeltaLake,
    DatabaseOptionsMongo, DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
    TableOptions, TableOptionsBigQuery, TableOptionsDebug, TableOptionsGcs, TableOptionsInternal,
    TableOptionsLocal, TableOptionsMongo, TableOptionsMysql, TableOptionsPostgres, TableOptionsS3,
    TableOptionsSnowflake, TunnelOptions,
};
use sqlbuiltins::builtins::{
    CURRENT_SESSION_SCHEMA, DATABASE_DEFAULT, DEFAULT_CATALOG, GLARE_COLUMNS, GLARE_CREDENTIALS,
    GLARE_DATABASES, GLARE_DEPLOYMENT_METADATA, GLARE_FUNCTIONS, GLARE_SCHEMAS,
    GLARE_SESSION_QUERY_METRICS, GLARE_SSH_KEYS, GLARE_TABLES, GLARE_TUNNELS, GLARE_VIEWS,
    SCHEMA_CURRENT_SESSION,
};
use std::str::FromStr;
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

    #[error("Missing tunnel connection: {0}")]
    MissingTunnel(u32),

    #[error("Invalid entry for table dispatch: {0}")]
    InvalidEntryTypeForDispatch(EntryType),

    #[error("Unhandled entry for table dispatch: {0:?}")]
    UnhandledEntry(EntryMeta),

    #[error("failed to do late planning: {0}")]
    LatePlanning(Box<crate::planner::errors::PlanError>),

    #[error("Invalid dispatch: {0}")]
    InvalidDispatch(&'static str),

    #[error(transparent)]
    RemoteDispatch(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    DebugDatasource(#[from] datasources::debug::errors::DebugError),
    #[error(transparent)]
    PostgresDatasource(#[from] datasources::postgres::errors::PostgresError),
    #[error(transparent)]
    BigQueryDatasource(#[from] datasources::bigquery::errors::BigQueryError),
    #[error(transparent)]
    MysqlDatasource(#[from] datasources::mysql::errors::MysqlError),
    #[error(transparent)]
    ObjectStoreDatasource(#[from] datasources::object_store::errors::ObjectStoreSourceError),
    #[error(transparent)]
    MongoDatasource(#[from] datasources::mongodb::errors::MongoError),
    #[error(transparent)]
    SnowflakeDatasource(#[from] datasources::snowflake::errors::DatasourceSnowflakeError),
    #[error(transparent)]
    DeltaDatasource(#[from] datasources::lake::delta::errors::DeltaError),
    #[error(transparent)]
    NativeDatasource(#[from] datasources::native::errors::NativeError),
    #[error(transparent)]
    CommonDatasource(#[from] datasources::common::errors::DatasourceCommonError),
    #[error(transparent)]
    SshKey(#[from] datasources::common::ssh::key::SshKeyError),
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
        // let catalog = catalog.read();
        // "External" database.
        if database != DEFAULT_CATALOG {
            let db = match catalog.resolve_database(database).await {
                Some(db) => db,
                None => {
                    return Err(DispatchError::MissingDatabase {
                        database: database.to_string(),
                    })
                }
            };
            return self.dispatch_external_database(&db, schema, name).await;
        }

        if schema == CURRENT_SESSION_SCHEMA {
            return match self.ctx.resolve_temp_table(name) {
                Some(table) => Ok(table),
                None => Err(DispatchError::MissingEntry {
                    schema: schema.to_owned(),
                    name: name.to_owned(),
                }),
            };
        }

        let ent = catalog
            .resolve_entry(database, schema, name)
            .await
            .ok_or_else(|| DispatchError::MissingEntry {
                schema: schema.to_string(),
                name: name.to_string(),
            })?;

        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(ent.entry_type(), EntryType::View | EntryType::Table) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        match &ent {
            CatalogEntry::View(view) => self.dispatch_view(view).await,
            // Dispatch to builtin tables.
            CatalogEntry::Table(tbl) if tbl.meta.builtin => {
                SystemTableDispatcher::new(self.ctx).dispatch(schema, name).await
            }
            // Dispatch to external tables.
            CatalogEntry::Table(tbl) if tbl.meta.external => {
                self.dispatch_external_table(tbl).await
            }
            // Dispatch to native tables.
            CatalogEntry::Table(tbl) => {
                let native = self.ctx.get_native_tables();
                let table = native.load_table(tbl).await?;
                Ok(table.into_table_provider())
            }
            other => Err(DispatchError::UnhandledEntry(other.get_meta().clone())),
        }
    }

    async fn dispatch_external_database(
        &self,
        db: &DatabaseEntry,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let tunnel = self.get_tunnel_opts(db.tunnel_id).await?;

        match &db.options {
            DatabaseOptions::Internal(_) => unimplemented!(),
            DatabaseOptions::Debug(DatabaseOptionsDebug {}) => {
                // Use name of the table as table type here.
                let provider = DebugTableType::from_str(name)?;
                Ok(provider.into_table_provider(tunnel.as_ref()))
            }
            DatabaseOptions::Postgres(DatabaseOptionsPostgres { connection_string }) => {
                let access = PostgresAccess::new_from_conn_str(connection_string, tunnel);
                let prov = PostgresTableProvider::try_new(access, None, schema, name).await?;
                Ok(Arc::new(prov))
            }
            DatabaseOptions::BigQuery(DatabaseOptionsBigQuery {
                service_account_key,
                project_id,
            }) => {
                let table_access = BigQueryTableAccess {
                    dataset_id: schema.to_string(),
                    table_id: name.to_string(),
                };

                let accessor =
                    BigQueryAccessor::connect(service_account_key.clone(), project_id.clone())
                        .await?;
                let provider = accessor.into_table_provider(table_access, true).await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::Mysql(DatabaseOptionsMysql { connection_string }) => {
                let table_access = MysqlTableAccess {
                    schema: schema.to_string(),
                    name: name.to_string(),
                };

                let accessor = MysqlAccessor::connect(connection_string, tunnel).await?;
                let provider = accessor.into_table_provider(table_access, true).await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::Mongo(DatabaseOptionsMongo { connection_string }) => {
                let table_info = MongoTableAccessInfo {
                    database: schema.to_string(), // A mongodb database is pretty much a schema.
                    collection: name.to_string(),
                };
                let accessor = MongoAccessor::connect(connection_string).await?;
                let table_accessor = accessor.into_table_accessor(table_info);
                let provider = table_accessor.into_table_provider().await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::Snowflake(DatabaseOptionsSnowflake {
                account_name,
                login_name,
                password,
                database_name,
                warehouse,
                role_name,
            }) => {
                let schema_name = schema.to_string();
                let table_name = name.to_string();
                let role_name = if role_name.is_empty() {
                    None
                } else {
                    Some(role_name.clone())
                };

                let conn_params = SnowflakeDbConnection {
                    account_name: account_name.clone(),
                    login_name: login_name.clone(),
                    password: password.clone(),
                    database_name: database_name.clone(),
                    warehouse: warehouse.clone(),
                    role_name,
                };
                let access_info = SnowflakeTableAccess {
                    schema_name,
                    table_name,
                };
                let accessor = SnowflakeAccessor::connect(conn_params).await?;
                let provider = accessor
                    .into_table_provider(access_info, /* predicate_pushdown = */ true)
                    .await?;
                Ok(Arc::new(provider))
            }
            DatabaseOptions::Delta(DatabaseOptionsDeltaLake {
                catalog,
                access_key_id,
                secret_access_key,
                region,
            }) => {
                let accessor =
                    DeltaLakeAccessor::connect(catalog, access_key_id, secret_access_key, region)
                        .await?;
                let table = accessor.load_table(schema, name).await?;
                Ok(Arc::new(table))
            }
        }
    }

    async fn dispatch_external_table(&self, table: &TableEntry) -> Result<Arc<dyn TableProvider>> {
        let tunnel = self.get_tunnel_opts(table.tunnel_id).await?;

        match &table.options {
            TableOptions::Internal(TableOptionsInternal { .. }) => unimplemented!(), // Purposely unimplemented.
            TableOptions::Debug(TableOptionsDebug { table_type }) => {
                let provider = DebugTableType::from_str(table_type)?;
                Ok(provider.into_table_provider(tunnel.as_ref()))
            }
            TableOptions::Postgres(TableOptionsPostgres {
                connection_string,
                schema,
                table,
            }) => {
                let access = PostgresAccess::new_from_conn_str(connection_string, tunnel);
                let prov = PostgresTableProvider::try_new(access, None, schema, table).await?;
                Ok(Arc::new(prov))
            }
            TableOptions::BigQuery(TableOptionsBigQuery {
                service_account_key,
                project_id,
                dataset_id,
                table_id,
            }) => {
                let table_access = BigQueryTableAccess {
                    dataset_id: dataset_id.to_string(),
                    table_id: table_id.to_string(),
                };

                let accessor =
                    BigQueryAccessor::connect(service_account_key.clone(), project_id.clone())
                        .await?;
                let provider = accessor.into_table_provider(table_access, true).await?;
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
                };

                let accessor = MysqlAccessor::connect(connection_string, tunnel).await?;
                let provider = accessor.into_table_provider(table_access, true).await?;
                Ok(Arc::new(provider))
            }
            TableOptions::Mongo(TableOptionsMongo {
                connection_string,
                database,
                collection,
            }) => {
                let table_info = MongoTableAccessInfo {
                    database: database.to_string(),
                    collection: collection.to_string(),
                };
                let accessor = MongoAccessor::connect(connection_string).await?;
                let table_accessor = accessor.into_table_accessor(table_info);
                let provider = table_accessor.into_table_provider().await?;
                Ok(Arc::new(provider))
            }
            TableOptions::Snowflake(TableOptionsSnowflake {
                account_name,
                login_name,
                password,
                database_name,
                warehouse,
                role_name,
                schema_name,
                table_name,
            }) => {
                let role_name = if role_name.is_empty() {
                    None
                } else {
                    Some(role_name.clone())
                };

                let conn_params = SnowflakeDbConnection {
                    account_name: account_name.clone(),
                    login_name: login_name.clone(),
                    password: password.clone(),
                    database_name: database_name.clone(),
                    warehouse: warehouse.clone(),
                    role_name,
                };
                let access_info = SnowflakeTableAccess {
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                };
                let accessor = SnowflakeAccessor::connect(conn_params).await?;
                let provider = accessor
                    .into_table_provider(access_info, /* predicate_pushdown = */ true)
                    .await?;
                Ok(Arc::new(provider))
            }
            TableOptions::Local(TableOptionsLocal {
                location,
                file_type,
                compression,
            }) => {
                if self.ctx.get_session_vars().is_cloud_instance() {
                    return Err(DispatchError::InvalidDispatch(
                        "Local file access is not supported in cloud mode",
                    ));
                }
                let access = Arc::new(LocalStoreAccess);
                self.create_obj_store_table_provider(
                    access,
                    location,
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
            TableOptions::Gcs(TableOptionsGcs {
                service_account_key,
                bucket,
                location,
                file_type,
                compression,
            }) => {
                let access = Arc::new(GcsStoreAccess {
                    service_account_key: service_account_key.clone(),
                    bucket: bucket.clone(),
                });
                self.create_obj_store_table_provider(
                    access,
                    location,
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
            TableOptions::S3(TableOptionsS3 {
                access_key_id,
                secret_access_key,
                region,
                bucket,
                location,
                file_type,
                compression,
            }) => {
                let access = Arc::new(S3StoreAccess {
                    region: region.clone(),
                    bucket: bucket.clone(),
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                });
                self.create_obj_store_table_provider(
                    access,
                    location,
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
        }
    }

    async fn create_obj_store_table_provider(
        &self,
        access: Arc<dyn ObjStoreAccess>,
        location: &str,
        file_type: &str,
        compression: Option<&String>,
    ) -> Result<Arc<dyn TableProvider>> {
        let compression = compression
            .map(|c| c.parse::<FileCompressionType>())
            .transpose()?
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let ft: FileType = file_type.parse()?;
        let ft: Arc<dyn FileFormat> = match ft {
            FileType::CSV => Arc::new(CsvFormat::default().with_file_compression_type(compression)),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::JSON => {
                Arc::new(JsonFormat::default().with_file_compression_type(compression))
            }
            _ => return Err(DispatchError::InvalidDispatch("Unsupported file type")),
        };

        let accessor = ObjStoreAccessor::new(access)?;
        let objects = accessor.list_globbed(location).await?;

        let state = self.ctx.df_ctx().state();
        let provider = accessor
            .into_table_provider(&state, ft, objects, /* predicate_pushdown = */ true)
            .await?;

        Ok(provider)
    }

    async fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .ctx
            .late_view_plan(&view.sql, &view.columns)
            .await
            .map_err(|e| DispatchError::LatePlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }

    async fn get_tunnel_opts(&self, tunnel_id: Option<u32>) -> Result<Option<TunnelOptions>> {
        let tunnel_options = if let Some(tunnel_id) = tunnel_id {
            let catalog = self.ctx.catalog();

            let ent = catalog
                .get_by_oid(tunnel_id)
                .await
                .ok_or(DispatchError::MissingTunnel(tunnel_id))?;

            let ent = match ent {
                CatalogEntry::Tunnel(ent) => ent,
                _ => return Err(DispatchError::MissingTunnel(tunnel_id)),
            };
            Some(ent.options.clone())
        } else {
            None
        };
        Ok(tunnel_options)
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

    fn catalog(&self) -> Arc<AsyncSessionCatalog> {
        self.ctx.get_session_catalog()
    }

    async fn dispatch(&self, schema: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        Ok(if GLARE_DATABASES.matches(schema, name) {
            Arc::new(self.build_glare_databases().await)
        } else if GLARE_TUNNELS.matches(schema, name) {
            Arc::new(self.build_glare_tunnels().await)
        } else if GLARE_CREDENTIALS.matches(schema, name) {
            Arc::new(self.build_glare_credentials().await)
        } else if GLARE_TABLES.matches(schema, name) {
            Arc::new(self.build_glare_tables().await)
        } else if GLARE_COLUMNS.matches(schema, name) {
            Arc::new(self.build_glare_columns().await)
        } else if GLARE_VIEWS.matches(schema, name) {
            Arc::new(self.build_glare_views().await)
        } else if GLARE_SCHEMAS.matches(schema, name) {
            Arc::new(self.build_glare_schemas().await)
        } else if GLARE_FUNCTIONS.matches(schema, name) {
            Arc::new(self.build_glare_functions().await)
        } else if GLARE_SESSION_QUERY_METRICS.matches(schema, name) {
            Arc::new(self.build_session_query_metrics())
        } else if GLARE_SSH_KEYS.matches(schema, name) {
            Arc::new(self.build_ssh_keys().await?)
        } else if GLARE_DEPLOYMENT_METADATA.matches(schema, name) {
            Arc::new(self.build_glare_deployment_metadata().await?)
        } else {
            return Err(DispatchError::MissingBuiltinTable {
                schema: schema.to_string(),
                name: name.to_string(),
            });
        })
    }

    async fn build_glare_databases(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_DATABASES.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();
        let mut datasource = StringBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for db in catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Database)
        {
            oid.append_value(db.oid);
            database_name.append_value(&db.entry.get_meta().name);
            builtin.append_value(db.builtin);
            external.append_value(db.entry.get_meta().external);

            let db = match db.entry {
                CatalogEntry::Database(db) => db,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            datasource.append_value(db.options.as_str());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(external.finish()),
                Arc::new(datasource.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    async fn build_glare_tunnels(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TUNNELS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut tunnel_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut tunnel_type = StringBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for tunnel in catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Tunnel)
        {
            oid.append_value(tunnel.oid);
            tunnel_name.append_value(&tunnel.entry.get_meta().name);
            builtin.append_value(tunnel.builtin);

            let tunnel = match tunnel.entry {
                CatalogEntry::Tunnel(tunnel) => tunnel,
                other => unreachable!("unexpected entry type: {other:?}"),
            };

            tunnel_type.append_value(tunnel.options.as_str());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(tunnel_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(tunnel_type.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    async fn build_glare_credentials(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_CREDENTIALS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut credentials_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut provider = StringBuilder::new();
        let mut comment = StringBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;
        for creds in catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Credentials)
        {
            oid.append_value(creds.oid);
            credentials_name.append_value(&creds.entry.get_meta().name);
            builtin.append_value(creds.builtin);

            let creds = match creds.entry {
                CatalogEntry::Credentials(creds) => creds,
                other => unreachable!("unexpected entry type: {other:?}"),
            };

            provider.append_value(creds.options.as_str());
            comment.append_value(&creds.comment);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(credentials_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(provider.finish()),
                Arc::new(comment.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    async fn build_glare_schemas(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SCHEMAS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut schema_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for schema in catalog
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

    async fn build_glare_tables(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TABLES.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut table_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();
        let mut datasource = StringBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for table in catalog
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

            let table = match table.entry {
                CatalogEntry::Table(table) => table,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            datasource.append_value(table.options.as_str());
        }

        // Append temporary tables.
        for table in self.ctx.list_temp_tables() {
            // TODO: Assign OID to temporary tables
            oid.append_value(0);
            schema_oid.append_value(SCHEMA_CURRENT_SESSION.oid);
            database_oid.append_value(DATABASE_DEFAULT.oid);
            schema_name.append_value(SCHEMA_CURRENT_SESSION.name);
            table_name.append_value(table);
            builtin.append_value(false);
            external.append_value(false);
            datasource.append_value("internal");
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
                Arc::new(datasource.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    async fn build_glare_columns(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_COLUMNS.arrow_schema());

        let mut schema_oid = UInt32Builder::new();
        let mut table_oid = UInt32Builder::new();
        let mut table_name = StringBuilder::new();
        let mut column_name = StringBuilder::new();
        let mut column_ordinal = UInt32Builder::new();
        let mut data_type = StringBuilder::new();
        let mut is_nullable = BooleanBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for table in catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            let ent = match table.entry {
                CatalogEntry::Table(ent) => ent,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            let cols = match ent.get_internal_columns() {
                Some(cols) => cols,
                None => continue,
            };

            for (i, col) in cols.iter().enumerate() {
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

    async fn build_glare_views(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_VIEWS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut view_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut sql = StringBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for view in catalog
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

    async fn build_glare_functions(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_FUNCTIONS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut function_name = StringBuilder::new();
        let mut function_type = StringBuilder::new();
        let mut parameters = ListBuilder::new(StringBuilder::new());
        let mut parameter_types = ListBuilder::new(StringBuilder::new());
        let mut builtin = BooleanBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for func in catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Function)
        {
            let ent = match func.entry {
                CatalogEntry::Function(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oid.append_value(func.oid);
            schema_oid.append_value(ent.meta.parent);
            function_name.append_value(&ent.meta.name);
            function_type.append_value(ent.func_type.as_str());

            // TODO: Actually get parameter info.
            const EMPTY: [Option<&'static str>; 0] = [];
            parameters.append_value(EMPTY);
            parameter_types.append_value(EMPTY);

            builtin.append_value(func.builtin);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(schema_oid.finish()),
                Arc::new(function_name.finish()),
                Arc::new(function_type.finish()),
                Arc::new(parameters.finish()),
                Arc::new(parameter_types.finish()),
                Arc::new(builtin.finish()),
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

    async fn build_ssh_keys(&self) -> Result<MemTable> {
        let arrow_schema = Arc::new(GLARE_SSH_KEYS.arrow_schema());

        let mut ssh_tunnel_oid = UInt32Builder::new();
        let mut ssh_tunnel_name = StringBuilder::new();
        let mut public_key = StringBuilder::new();
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        for t in catalog.iter_entries().filter(|ent| match ent.entry {
            CatalogEntry::Tunnel(tunnel_entry) => {
                matches!(tunnel_entry.options, TunnelOptions::Ssh(_))
            }
            _ => false,
        }) {
            ssh_tunnel_oid.append_value(t.oid);
            ssh_tunnel_name.append_value(&t.entry.get_meta().name);

            match t.entry {
                CatalogEntry::Tunnel(tunnel_entry) => match &tunnel_entry.options {
                    TunnelOptions::Ssh(ssh_options) => {
                        let key = SshKey::from_bytes(&ssh_options.ssh_key)?;
                        let key = key.public_key()?;
                        let conn_params: SshConnectionParameters =
                            ssh_options.connection_string.parse()?;
                        let key = format!("{} {}", key, conn_params.user);
                        public_key.append_value(key);
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(ssh_tunnel_oid.finish()),
                Arc::new(ssh_tunnel_name.finish()),
                Arc::new(public_key.finish()),
            ],
        )
        .unwrap();

        Ok(MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap())
    }

    async fn build_glare_deployment_metadata(&self) -> Result<MemTable> {
        let arrow_schema = Arc::new(GLARE_DEPLOYMENT_METADATA.arrow_schema());
        let catalog = self.catalog();
        let catalog = catalog.read().await;

        let deployment = catalog.deployment_metadata();

        let (mut key, mut value): (StringBuilder, StringBuilder) = [(
            Some("storage_size"),
            Some(deployment.storage_size.to_string()),
        )]
        .into_iter()
        .unzip();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(key.finish()), Arc::new(value.finish())],
        )
        .unwrap();

        Ok(MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap())
    }
}
