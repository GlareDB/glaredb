use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::{FileCompressionType, FileType};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider, VirtualLister};
use datafusion_ext::vars::SessionVars;
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::common::url::DatasourceUrl;
use datasources::debug::DebugTableType;
use datasources::lake::delta::access::{load_table_direct, DeltaLakeAccessor};
use datasources::lake::iceberg::table::IcebergTable;
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::generic::GenericStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::{ObjStoreAccess, ObjStoreAccessor};
use datasources::postgres::{PostgresAccess, PostgresTableProvider, PostgresTableProviderConfig};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use protogen::metastore::types::catalog::{
    CatalogEntry, CredentialsEntry, DatabaseEntry, FunctionEntry, TableEntry,
};
use protogen::metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsDebug, DatabaseOptionsDeltaLake,
    DatabaseOptionsMongo, DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
    TableOptions, TableOptionsBigQuery, TableOptionsDebug, TableOptionsGcs, TableOptionsInternal,
    TableOptionsLocal, TableOptionsMongo, TableOptionsMysql, TableOptionsObjectStore,
    TableOptionsPostgres, TableOptionsS3, TableOptionsSnowflake, TunnelOptions,
};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use sqlbuiltins::functions::BUILTIN_TABLE_FUNCS;

use crate::metastore::catalog::SessionCatalog;

use super::listing::CatalogLister;
use super::{DispatchError, Result};

/// Dispatch to external tables and databases.
pub struct ExternalDispatcher<'a> {
    catalog: &'a SessionCatalog,
    // TODO: Remove need for this.
    df_ctx: &'a SessionContext,
    /// Whether or not local file system access should be disabled.
    disable_local_fs_access: bool,
}

impl<'a> TableFuncContextProvider for ExternalDispatcher<'a> {
    fn get_database_entry(&self, name: &str) -> Option<&DatabaseEntry> {
        self.catalog.resolve_database(name)
    }

    fn get_credentials_entry(&self, name: &str) -> Option<&CredentialsEntry> {
        self.catalog.resolve_credentials(name)
    }

    fn get_session_vars(&self) -> SessionVars {
        let cfg = self.df_ctx.copied_config();
        let vars = cfg.options().extensions.get::<SessionVars>().unwrap();
        vars.clone()
    }

    fn get_session_state(&self) -> SessionState {
        self.df_ctx.state()
    }

    fn get_catalog_lister(&self) -> Box<dyn VirtualLister> {
        Box::new(CatalogLister::new(self.catalog.clone(), true))
    }
}

impl<'a> ExternalDispatcher<'a> {
    pub fn new(
        catalog: &'a SessionCatalog,
        df_ctx: &'a SessionContext,
        disable_local_fs_access: bool,
    ) -> Self {
        ExternalDispatcher {
            catalog,
            df_ctx,
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

        match &db.options {
            DatabaseOptions::Internal(_) => unimplemented!(),
            DatabaseOptions::Debug(DatabaseOptionsDebug {}) => {
                // Use name of the table as table type here.
                let provider = DebugTableType::from_str(name)?;
                Ok(provider.into_table_provider(tunnel.as_ref()))
            }
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
                storage_options,
            }) => {
                let accessor = DeltaLakeAccessor::connect(catalog, storage_options.clone()).await?;
                let table = accessor.load_table(schema, name).await?;
                Ok(Arc::new(table))
            }
        }
    }

    pub async fn dispatch_external_table(
        &self,
        table: &TableEntry,
    ) -> Result<Arc<dyn TableProvider>> {
        let tunnel = self.get_tunnel_opts(table.tunnel_id)?;

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
                let prov_conf = PostgresTableProviderConfig {
                    access,
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                };
                let prov = PostgresTableProvider::try_new(prov_conf).await?;
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
            TableOptions::Azure(TableOptionsObjectStore {
                location,
                storage_options,
                file_type,
                compression,
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

                let access = Arc::new(GenericStoreAccess::new_from_location_and_opts(
                    location,
                    storage_options.clone(),
                )?);
                self.create_obj_store_table_provider(
                    access,
                    location,
                    file_type,
                    compression.as_ref(),
                )
                .await
            }
            TableOptions::Delta(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            }) => {
                let provider =
                    Arc::new(load_table_direct(location, storage_options.clone()).await?);
                Ok(provider)
            }
            TableOptions::Iceberg(TableOptionsObjectStore {
                location,
                storage_options,
                ..
            }) => {
                let url = DatasourceUrl::try_new(location)?;
                let store = GenericStoreAccess::new_from_location_and_opts(
                    location,
                    storage_options.clone(),
                )?
                .create_store()?;
                let table = IcebergTable::open(url, store).await?;
                let reader = table.table_reader().await?;
                Ok(reader)
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
            FileType::CSV => Arc::new(
                CsvFormat::default()
                    .with_file_compression_type(compression)
                    .with_schema_infer_max_rec(Some(20480)),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::JSON => {
                Arc::new(JsonFormat::default().with_file_compression_type(compression))
            }
            _ => return Err(DispatchError::InvalidDispatch("Unsupported file type")),
        };

        let accessor = ObjStoreAccessor::new(access)?;
        let objects = accessor.list_globbed(location).await?;

        let state = self.df_ctx.state();
        let provider = accessor.into_table_provider(&state, ft, objects).await?;

        Ok(provider)
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
            BUILTIN_TABLE_FUNCS.find_function(&func.meta.name)
        } else {
            // We only have builtin functions right now.
            None
        };
        let prov = resolve_func
            .unwrap()
            .create_provider(self, args, opts)
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
}
