use std::collections::HashMap;
use std::sync::Arc;

use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_ext::functions::{DefaultTableContextProvider, FuncParamValue};
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::bson::table::bson_streaming_table;
use datasources::cassandra::CassandraTableProvider;
use datasources::clickhouse::{ClickhouseAccess, ClickhouseTableProvider, OwnedClickhouseTableRef};
use datasources::common::url::DatasourceUrl;
use datasources::debug::DebugTableProvider;
use datasources::excel::table::ExcelTableProvider;
use datasources::excel::ExcelTable;
use datasources::lake::delta::access::{load_table_direct, DeltaLakeAccessor};
use datasources::lake::iceberg::table::IcebergTable;
use datasources::lake::{storage_options_into_object_store, storage_options_into_store_access};
use datasources::lance::LanceTable;
use datasources::mongodb::{MongoDbAccessor, MongoDbTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::azure::AzureStoreAccess;
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::{ObjStoreAccess, ObjStoreAccessor};
use datasources::postgres::{PostgresAccess, PostgresTableProvider, PostgresTableProviderConfig};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use datasources::sqlite::{SqliteAccess, SqliteTableProvider};
use datasources::sqlserver::{
    SqlServerAccess,
    SqlServerTableProvider,
    SqlServerTableProviderConfig,
};
use protogen::metastore::types::catalog::{CatalogEntry, DatabaseEntry, FunctionEntry, TableEntry};
use protogen::metastore::types::options::{
    DatabaseOptions,
    DatabaseOptionsBigQuery,
    DatabaseOptionsCassandra,
    DatabaseOptionsClickhouse,
    DatabaseOptionsDebug,
    DatabaseOptionsDeltaLake,
    DatabaseOptionsMongoDb,
    DatabaseOptionsMysql,
    DatabaseOptionsPostgres,
    DatabaseOptionsSnowflake,
    DatabaseOptionsSqlServer,
    DatabaseOptionsSqlite,
    TableOptionsBigQuery,
    TableOptionsCassandra,
    TableOptionsClickhouse,
    TableOptionsExcel,
    TableOptionsGcs,
    TableOptionsInternal,
    TableOptionsLocal,
    TableOptionsMongoDb,
    TableOptionsMysql,
    TableOptionsObjectStore,
    TableOptionsPostgres,
    TableOptionsS3,
    TableOptionsSnowflake,
    TableOptionsSqlServer,
    TableOptionsSqlite,
    TableOptionsV0,
    TunnelOptions,
};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use sqlbuiltins::functions::FunctionRegistry;
use sqlbuiltins::DEFAULT_DATASOURCES;

use super::{DispatchError, Result};

/// Dispatch to external tables and databases.
// TODO: add a `DatasourceRegistry` to the `ExternalDispatcher` to allow for dynamic datasources.
pub struct ExternalDispatcher<'a> {
    catalog: &'a SessionCatalog,
    // TODO: Remove need for this.
    df_ctx: &'a SessionContext,

    function_registry: &'a FunctionRegistry,
    /// Whether or not local file system access should be disabled.
    disable_local_fs_access: bool,
}

impl<'a> ExternalDispatcher<'a> {
    pub fn new(
        catalog: &'a SessionCatalog,
        df_ctx: &'a SessionContext,
        function_registry: &'a FunctionRegistry,
        disable_local_fs_access: bool,
    ) -> Self {
        ExternalDispatcher {
            catalog,
            df_ctx,
            function_registry,
            disable_local_fs_access,
        }
    }

    pub async fn dispatch_external(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        if database != DEFAULT_CATALOG {
            let db = match self.catalog.resolve_database(database) {
                Some(db) => db,
                None => {
                    return Err(DispatchError::MissingDatabase {
                        database: database.to_string(),
                    })
                }
            };
            return self.dispatch_external_database(db, schema, name).await;
        }

        let ent = self
            .catalog
            .resolve_entry(database, schema, name)
            .ok_or_else(|| DispatchError::MissingEntry {
                schema: schema.to_string(),
                name: name.to_string(),
            })?;

        if !ent.get_meta().external {
            return Err(DispatchError::InvalidDispatch("table not external"));
        }

        let table = match ent {
            CatalogEntry::Table(table) => table,
            _ => return Err(DispatchError::InvalidDispatch("entry is not a table")),
        };

        self.dispatch_external_table(table).await
    }

    pub async fn dispatch_external_database(
        &self,
        db: &DatabaseEntry,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let tunnel = self.get_tunnel_opts(db.tunnel_id)?;
        // TODO: use the DatasourceRegistry to dispatch instead
        match &db.options {
            DatabaseOptions::Debug(DatabaseOptionsDebug {}) => {
                let tbl_type = name.parse()?;
                Ok(Arc::new(DebugTableProvider {
                    typ: tbl_type,
                    tunnel: tunnel.is_some(),
                }))
            }
            DatabaseOptions::Internal(_) => unimplemented!(),
            DatabaseOptions::Postgres(DatabaseOptionsPostgres { connection_string }) => {
                let access = PostgresAccess::new_from_conn_str(connection_string, tunnel);
                let prov_conf = PostgresTableProviderConfig {
                    access,
                    schema: schema.to_owned(),
                    table: name.to_owned(),
                };
                let prov = PostgresTableProvider::try_new(prov_conf).await?;
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
            DatabaseOptions::MongoDb(DatabaseOptionsMongoDb { connection_string }) => {
                let table_info = MongoDbTableAccessInfo {
                    database: schema.to_string(), // A mongodb database is pretty much a schema.
                    collection: name.to_string(),
                    fields: None,
                };
                let accessor = MongoDbAccessor::connect(connection_string).await?;
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
                storage_options,
            }) => {
                let accessor = DeltaLakeAccessor::connect(catalog, storage_options.clone()).await?;
                let table = accessor.load_table(schema, name).await?;
                Ok(Arc::new(table))
            }
            DatabaseOptions::SqlServer(DatabaseOptionsSqlServer { connection_string }) => {
                let access = SqlServerAccess::try_new_from_ado_string(connection_string)?;
                let table = SqlServerTableProvider::try_new(SqlServerTableProviderConfig {
                    access,
                    schema: schema.to_string(),
                    table: name.to_string(),
                })
                .await?;
                Ok(Arc::new(table))
            }
            DatabaseOptions::Clickhouse(DatabaseOptionsClickhouse { connection_string }) => {
                let access =
                    ClickhouseAccess::new_from_connection_string(connection_string.clone());
                let table_ref =
                    OwnedClickhouseTableRef::new(Some(schema.to_owned()), name.to_owned());
                let table = ClickhouseTableProvider::try_new(access, table_ref).await?;
                Ok(Arc::new(table))
            }
            DatabaseOptions::Cassandra(DatabaseOptionsCassandra {
                host,
                username,
                password,
            }) => {
                let table = CassandraTableProvider::try_new(
                    host.clone(),
                    schema.to_string(),
                    name.to_string(),
                    username.to_owned(),
                    password.to_owned(),
                )
                .await?;
                Ok(Arc::new(table))
            }
            DatabaseOptions::Sqlite(DatabaseOptionsSqlite { location }) => {
                let access = SqliteAccess {
                    db: location.into(),
                };
                let state = access.connect().await?;
                let table = SqliteTableProvider::try_new(state, name).await?;
                Ok(Arc::new(table))
            }
        }
    }


    pub async fn dispatch_external_table(
        &self,
        table: &TableEntry,
    ) -> Result<Arc<dyn TableProvider>> {
        let tunnel = self.get_tunnel_opts(table.tunnel_id)?;
        let columns_opt = table.columns.clone();
        let optional_schema = columns_opt.map(|cols| {
            let fields: Vec<Field> = cols
                .into_iter()
                .map(|col| {
                    let fld: Field = col.into();
                    fld
                })
                .collect();
            Schema::new(fields)
        });
        if let Some(ds) = DEFAULT_DATASOURCES.get(&table.options.name) {
            ds.create_table_provider(&table.options, tunnel.as_ref())
                .await
                .map_err(|e| e.into())
        } else {
            let tbl_options_old = TableOptionsV0::try_from(&table.options)
                .map_err(|e| DispatchError::String(format!("Invalid table options: {}", e)))?;
            self.dispatch_table_options_v1(&tbl_options_old, tunnel, optional_schema)
                .await
        }
    }

    async fn create_obj_store_table_provider(
        &self,
        access: Arc<dyn ObjStoreAccess>,
        path: impl AsRef<str>,
        file_type: &str,
        compression: Option<&String>,
    ) -> Result<Arc<dyn TableProvider>> {
        let path = path.as_ref();
        let compression = compression
            .map(|c| c.parse::<FileCompressionType>())
            .transpose()?
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let accessor = ObjStoreAccessor::new(access.clone())?;

        match file_type {
            "csv" => Ok(accessor
                .clone()
                .into_table_provider(
                    &self.df_ctx.state(),
                    Arc::new(
                        CsvFormat::default()
                            .with_file_compression_type(compression)
                            .with_schema_infer_max_rec(Some(20480)),
                    ),
                    accessor.clone().list_globbed(path).await?,
                )
                .await?),
            "parquet" => Ok(accessor
                .clone()
                .into_table_provider(
                    &self.df_ctx.state(),
                    Arc::new(ParquetFormat::default()),
                    accessor.clone().list_globbed(path).await?,
                )
                .await?),
            "ndjson" | "json" => Ok(accessor
                .clone()
                .into_table_provider(
                    &self.df_ctx.state(),
                    Arc::new(JsonFormat::default().with_file_compression_type(compression)),
                    accessor.clone().list_globbed(path).await?,
                )
                .await?),
            "bson" => Ok(bson_streaming_table(
                access.clone(),
                DatasourceUrl::try_new(path)?,
                None,
                Some(128),
            )
            .await?),
            _ => Err(DispatchError::String(
                format!("Unsupported file type: '{}', for '{}'", file_type, path,).to_string(),
            )),
        }
    }

    pub async fn dispatch_function(
        &self,
        func: &FunctionEntry,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let args = args.unwrap_or_default();
        let opts = opts.unwrap_or_default();
        let resolve_func = if func.meta.builtin {
            self.function_registry.get_table_func(&func.meta.name)
        } else {
            // We only have builtin functions right now.
            None
        };
        let prov = resolve_func
            .unwrap()
            .create_provider(
                &DefaultTableContextProvider::new(self.catalog, self.df_ctx),
                args,
                opts,
            )
            .await?;
        Ok(prov)
    }

    fn get_tunnel_opts(&self, tunnel_id: Option<u32>) -> Result<Option<TunnelOptions>> {
        let tunnel_options = if let Some(tunnel_id) = tunnel_id {
            let ent = self
                .catalog
                .get_by_oid(tunnel_id)
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

    // TODO: Remove this function once everything is using the new table options
    async fn dispatch_table_options_v1(
        &self,
        opts: &TableOptionsV0,
        tunnel: Option<TunnelOptions>,
        schema: Option<Schema>,
    ) -> Result<Arc<dyn TableProvider>> {
        match &opts {
            TableOptionsV0::Debug(dbg) => Ok(Arc::new(DebugTableProvider {
                typ: dbg.table_type.parse()?,
                tunnel: tunnel.is_some(),
            })),
            TableOptionsV0::Internal(TableOptionsInternal { .. }) => unimplemented!(), // Purposely unimplemented.
            TableOptionsV0::Excel(TableOptionsExcel {
                location,
                storage_options,
                has_header,
                sheet_name,
                ..
            }) => {
                let source_url = DatasourceUrl::try_new(location)?;
                let store_access = storage_options_into_store_access(&source_url, storage_options)?;
                let sheet_name: Option<&str> = sheet_name.as_deref();

                let table =
                    ExcelTable::open(store_access, source_url, sheet_name, *has_header).await?;
                let provider = ExcelTableProvider::try_new(table).await?;

                Ok(Arc::new(provider))
            }

            TableOptionsV0::Postgres(TableOptionsPostgres {
                connection_string,
                schema,
                table,
            }) => {
                let access = PostgresAccess::new_from_conn_str(connection_string, tunnel);
                let prov_conf = PostgresTableProviderConfig {
                    access,
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                };
                let prov = PostgresTableProvider::try_new(prov_conf).await?;
                Ok(Arc::new(prov))
            }
            TableOptionsV0::BigQuery(TableOptionsBigQuery {
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
            TableOptionsV0::Mysql(TableOptionsMysql {
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
            TableOptionsV0::MongoDb(TableOptionsMongoDb {
                connection_string,
                database,
                collection,
            }) => {
                let table_info = MongoDbTableAccessInfo {
                    database: database.to_string(),
                    collection: collection.to_string(),
                    fields: None,
                };
                let accessor = MongoDbAccessor::connect(connection_string).await?;
                let table_accessor = accessor.into_table_accessor(table_info);
                let provider = table_accessor.into_table_provider().await?;
                Ok(Arc::new(provider))
            }
            TableOptionsV0::Snowflake(TableOptionsSnowflake {
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
            TableOptionsV0::Local(TableOptionsLocal {
                location,
                file_type,
                compression,
            }) => {
                if self.disable_local_fs_access {
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
            TableOptionsV0::Gcs(TableOptionsGcs {
                service_account_key,
                bucket,
                location,
                file_type,
                compression,
            }) => {
                let access = Arc::new(GcsStoreAccess {
                    service_account_key: service_account_key.clone(),
                    bucket: bucket.clone(),
                    opts: HashMap::new(),
                });
                self.create_obj_store_table_provider(
                    access,
                    location,
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
            TableOptionsV0::S3(TableOptionsS3 {
                access_key_id,
                secret_access_key,
                region,
                bucket,
                location,
                file_type,
                compression,
            }) => {
                let access = Arc::new(S3StoreAccess {
                    bucket: bucket.clone(),
                    region: Some(region.clone()),
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    opts: HashMap::new(),
                });
                self.create_obj_store_table_provider(
                    access,
                    location,
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
            TableOptionsV0::Azure(TableOptionsObjectStore {
                location,
                storage_options,
                file_type,
                compression,
                ..
            }) => {
                // File type should be known at this point since creating the
                // table requires that we've either inferred the file type, or
                // the user provided it.
                let file_type = match file_type {
                    Some(ft) => ft,
                    None => {
                        return Err(DispatchError::InvalidDispatch(
                            "File type missing from table options",
                        ))
                    }
                };

                let uri = DatasourceUrl::try_new(location)?;
                let access = Arc::new(AzureStoreAccess::try_from_uri(&uri, storage_options)?);

                self.create_obj_store_table_provider(
                    access,
                    DatasourceUrl::try_new(location)?.path(), // TODO: Workaround again
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
            TableOptionsV0::Delta(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            }) => {
                let provider =
                    Arc::new(load_table_direct(location, storage_options.clone()).await?);
                Ok(provider)
            }
            TableOptionsV0::Iceberg(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            }) => {
                let url = DatasourceUrl::try_new(location)?;
                let store = storage_options_into_object_store(&url, storage_options)?;
                let table = IcebergTable::open(url, store).await?;
                let reader = table.table_reader().await?;
                Ok(reader)
            }
            TableOptionsV0::SqlServer(TableOptionsSqlServer {
                connection_string,
                schema,
                table,
            }) => {
                let access = SqlServerAccess::try_new_from_ado_string(connection_string)?;
                let table = SqlServerTableProvider::try_new(SqlServerTableProviderConfig {
                    access,
                    schema: schema.to_string(),
                    table: table.to_string(),
                })
                .await?;
                Ok(Arc::new(table))
            }
            TableOptionsV0::Clickhouse(TableOptionsClickhouse {
                connection_string,
                database,
                table,
            }) => {
                let access =
                    ClickhouseAccess::new_from_connection_string(connection_string.clone());
                let table_ref = OwnedClickhouseTableRef::new(database.clone(), table.to_owned());
                let table = ClickhouseTableProvider::try_new(access, table_ref).await?;
                Ok(Arc::new(table))
            }
            TableOptionsV0::Lance(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            }) => Ok(Arc::new(
                LanceTable::new(location, storage_options.clone()).await?,
            )),
            TableOptionsV0::Bson(TableOptionsObjectStore {
                location,
                storage_options,
                schema_sample_size,
                ..
            }) => {
                let source_url = DatasourceUrl::try_new(location)?;
                let store_access = storage_options_into_store_access(&source_url, storage_options)?;
                Ok(bson_streaming_table(
                    store_access,
                    source_url,
                    schema,
                    schema_sample_size.to_owned(),
                )
                .await?)
            }
            TableOptionsV0::Cassandra(TableOptionsCassandra {
                host,
                keyspace,
                table,
                username,
                password,
            }) => {
                let table = CassandraTableProvider::try_new(
                    host.clone(),
                    keyspace.clone(),
                    table.clone(),
                    username.clone(),
                    password.clone(),
                )
                .await?;

                Ok(Arc::new(table))
            }
            TableOptionsV0::Sqlite(TableOptionsSqlite { location, table }) => {
                let access = SqliteAccess {
                    db: location.into(),
                };
                let state = access.connect().await?;
                let table = SqliteTableProvider::try_new(state, table).await?;
                Ok(Arc::new(table))
            }
        }
    }
}
