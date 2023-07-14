//! Builtin table returning functions.
use crate::errors::{BuiltinError, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::streaming::{PartitionStream, StreamingTable};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::sql::sqlparser::ast::FunctionArg;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg as SqlFunctionArg, Value as SqlValue,
};
use datafusion::{arrow::datatypes::DataType, datasource::TableProvider};
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::common::listing::VirtualLister;
use datasources::debug::DebugVirtualLister;
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasources::object_store::http::HttpAccessor;
use datasources::object_store::local::{LocalAccessor, LocalTableAccess};
use datasources::object_store::{FileType, TableAccessor};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use futures::Stream;
use metastoreproto::types::catalog::DatabaseEntry;
use metastoreproto::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo, DatabaseOptionsMysql,
    DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
};
use num_traits::Zero;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::ops::{Add, AddAssign};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use url::Url;

/// Builtin table returning functions available for all sessions.
pub static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);

/// A single parameter for a table function.
#[derive(Debug, Clone)]
pub struct TableFuncParameter {
    pub name: &'static str,
    pub typ: DataType,
}

/// A set of parameters for a table function.
#[derive(Debug, Clone)]
pub struct TableFuncParameters {
    pub params: &'static [TableFuncParameter],
}

/// All builtin table functions.
pub struct BuiltinTableFuncs {
    funcs: HashMap<String, Arc<dyn TableFunc>>,
}

impl BuiltinTableFuncs {
    pub fn new() -> BuiltinTableFuncs {
        let funcs: Vec<Arc<dyn TableFunc>> = vec![
            // Read from table sources
            Arc::new(ReadPostgres),
            Arc::new(ReadBigQuery),
            Arc::new(ReadMongoDb),
            Arc::new(ReadMysql),
            Arc::new(ReadSnowflake),
            Arc::new(ParquetScan),
            Arc::new(CsvScan),
            Arc::new(JsonScan),
            // Listing
            Arc::new(ListSchemas),
            Arc::new(ListTables),
            // Series generating
            Arc::new(GenerateSeries),
        ];
        let funcs: HashMap<String, Arc<dyn TableFunc>> = funcs
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        BuiltinTableFuncs { funcs }
    }

    pub fn find_function(&self, name: &str) -> Option<Arc<dyn TableFunc>> {
        self.funcs.get(name).cloned()
    }

    pub fn iter_funcs(&self) -> impl Iterator<Item = &Arc<dyn TableFunc>> {
        self.funcs.values()
    }
}

impl Default for BuiltinTableFuncs {
    fn default() -> Self {
        Self::new()
    }
}

pub trait TableFuncContextProvider: Sync + Send {
    fn get_database_entry(&self, name: &str) -> Option<&DatabaseEntry>;
}

#[async_trait]
pub trait TableFunc: Sync + Send {
    /// The name for this table function. This name will be used when looking up
    /// function implementations.
    fn name(&self) -> &str;

    /// A list of function parameters.
    ///
    /// Note that returns a slice to allow for functions that take a variable
    /// number of arguments. For example, a function implementation might allow
    /// 2 or 3 parameters. The same implementation would be able to handle both
    /// of these calls:
    ///
    /// my_func(arg1, arg2)
    /// my_func(arg1, arg2, arg3)
    fn parameters(&self) -> &[TableFuncParameters];

    /// Return a table provider using the provided args.
    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>>;
}

#[derive(Debug, Clone, Copy)]
pub struct ReadPostgres;

#[async_trait]
impl TableFunc for ReadPostgres {
    fn name(&self) -> &str {
        "read_postgres"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "connection_str",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.iter();
                let conn_str: String = args.next().unwrap().extract()?;
                let schema: String = args.next().unwrap().extract()?;
                let table: String = args.next().unwrap().extract()?;

                let access = PostgresAccessor::connect(&conn_str, None)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        PostgresTableAccess {
                            schema: schema.clone(),
                            name: table.clone(),
                        },
                        true,
                    )
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadBigQuery;

#[async_trait]
impl TableFunc for ReadBigQuery {
    fn name(&self) -> &str {
        "read_bigquery"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "gcp_service_account_key",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "project_id",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "dataset_id",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table_id",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            4 => {
                let mut args = args.iter();
                let service_account = args.next().unwrap().extract()?;
                let project_id = args.next().unwrap().extract()?;
                let dataset_id = args.next().unwrap().extract()?;
                let table_id = args.next().unwrap().extract()?;

                let access = BigQueryAccessor::connect(service_account, project_id)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        BigQueryTableAccess {
                            dataset_id,
                            table_id,
                        },
                        true,
                    )
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadMongoDb;

#[async_trait]
impl TableFunc for ReadMongoDb {
    fn name(&self) -> &str {
        "read_mongodb"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "connection_str",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "database",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "collection",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.iter();
                let conn_str: String = args.next().unwrap().extract()?;
                let database = args.next().unwrap().extract()?;
                let collection = args.next().unwrap().extract()?;

                let access = MongoAccessor::connect(&conn_str)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_accessor(MongoTableAccessInfo {
                        database,
                        collection,
                    })
                    .into_table_provider()
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadMysql;

#[async_trait]
impl TableFunc for ReadMysql {
    fn name(&self) -> &str {
        "read_mysql"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "connection_str",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.iter();
                let conn_str: String = args.next().unwrap().extract()?;
                let schema: String = args.next().unwrap().extract()?;
                let table: String = args.next().unwrap().extract()?;

                let access = MysqlAccessor::connect(&conn_str, None)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        MysqlTableAccess {
                            schema: schema.clone(),
                            name: table.clone(),
                        },
                        true,
                    )
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadSnowflake;

#[async_trait]
impl TableFunc for ReadSnowflake {
    fn name(&self) -> &str {
        "read_snowflake"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "account",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "username",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "password",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "database",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "warehouse",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "role",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            8 => {
                let mut args = args.iter();
                let account = args.next().unwrap().extract()?;
                let username = args.next().unwrap().extract()?;
                let password = args.next().unwrap().extract()?;
                let database = args.next().unwrap().extract()?;
                let warehouse = args.next().unwrap().extract()?;
                let role = args.next().unwrap().extract()?;
                let schema = args.next().unwrap().extract()?;
                let table = args.next().unwrap().extract()?;

                let conn_params = SnowflakeDbConnection {
                    account_name: account,
                    login_name: username,
                    password,
                    database_name: database,
                    warehouse,
                    role_name: Some(role),
                };
                let access_info = SnowflakeTableAccess {
                    schema_name: schema,
                    table_name: table,
                };
                let accessor = SnowflakeAccessor::connect(conn_params)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = accessor
                    .into_table_provider(access_info, true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParquetScan;

#[async_trait]
impl TableFunc for ParquetScan {
    fn name(&self) -> &str {
        "parquet_scan"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[TableFuncParameter {
                name: "url",
                typ: DataType::Utf8,
            }],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        create_provider_for_filetype(FileType::Parquet, args).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CsvScan;

#[async_trait]
impl TableFunc for CsvScan {
    fn name(&self) -> &str {
        "csv_scan"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[TableFuncParameter {
                name: "url",
                typ: DataType::Utf8,
            }],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        create_provider_for_filetype(FileType::Csv, args).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JsonScan;

#[async_trait]
impl TableFunc for JsonScan {
    fn name(&self) -> &str {
        "ndjson_scan"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[TableFuncParameter {
                name: "url",
                typ: DataType::Utf8,
            }],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        create_provider_for_filetype(FileType::Json, args).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;

async fn create_provider_for_filetype(
    file_type: FileType,
    args: &[FunctionArg],
) -> Result<Arc<dyn TableProvider>> {
    match args.len() {
        1 => {
            let mut args = args.iter();
            let url_string: String = args.next().unwrap().extract()?;

            Ok(match Url::parse(&url_string).as_ref().map(Url::scheme) {
                Ok("http" | "https") => HttpAccessor::try_new(url_string, file_type)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(false)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?,
                Ok("gs") => return Err(BuiltinError::InvalidNumArgs),
                // no scheme so we assume it's a local file
                _ => {
                    let location = url_string
                        .strip_prefix("file://")
                        // if it's not a file url, we assume it's a local file
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| url_string);

                    let table_access = LocalTableAccess {
                        location,
                        file_type: Some(file_type),
                    };
                    LocalAccessor::new(table_access)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
            })
        }
        2 => {
            let mut args = args.iter();
            let url_string: String = args.next().unwrap().extract()?;
            Ok(match Url::parse(&url_string).as_ref().map(Url::scheme) {
                Ok("gs") => {
                    let creds = args.next().unwrap().extract()?;
                    let url = Url::parse(&url_string).unwrap();

                    let bucket = url.host_str().unwrap().to_string();
                    let location = url.path().to_string();

                    let access = GcsTableAccess {
                        bucket_name: bucket,
                        location,
                        service_acccount_key_json: Some(creds),
                        file_type: Some(file_type),
                    };
                    GcsAccessor::new(access)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                _ => return Err(BuiltinError::InvalidNumArgs),
            })
        }
        _ => Err(BuiltinError::InvalidNumArgs),
    }
}
#[async_trait]
impl TableFunc for ListSchemas {
    fn name(&self) -> &str {
        "list_schemas"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[TableFuncParameter {
                name: "database",
                typ: DataType::Utf8,
            }],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            1 => {
                let mut args = args.iter();
                let database = args.next().unwrap().extract()?;

                let fields = vec![Field::new("schema_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_db_lister(ctx, database).await?;
                let schema_list = lister
                    .list_schemas()
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let schema_list: StringArray = schema_list.into_iter().map(Some).collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(schema_list)])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListTables;

#[async_trait]
impl TableFunc for ListTables {
    fn name(&self) -> &str {
        "list_tables"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "database",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.iter();
                let database = args.next().unwrap().extract()?;
                let schema_name: String = args.next().unwrap().extract()?;

                let fields = vec![Field::new("table_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_db_lister(ctx, database).await?;
                let tables_list = lister
                    .list_tables(&schema_name)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let tables_list: StringArray = tables_list.into_iter().map(Some).collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(tables_list)])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}

async fn get_db_lister(
    ctx: &dyn TableFuncContextProvider,
    dbname: String,
) -> Result<Box<dyn VirtualLister>> {
    let db = ctx
        .get_database_entry(&dbname)
        .ok_or(BuiltinError::MissingObject {
            obj_typ: "database",
            name: dbname,
        })?;

    let lister: Box<dyn VirtualLister> = match &db.options {
        DatabaseOptions::Internal(_) => unimplemented!(),
        DatabaseOptions::Debug(_) => Box::new(DebugVirtualLister),
        DatabaseOptions::Postgres(DatabaseOptionsPostgres { connection_string }) => {
            let accessor = PostgresAccessor::connect(connection_string, None)
                .await
                .map_err(|e| BuiltinError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::BigQuery(DatabaseOptionsBigQuery {
            service_account_key,
            project_id,
        }) => {
            let accessor =
                BigQueryAccessor::connect(service_account_key.clone(), project_id.clone())
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::Mysql(DatabaseOptionsMysql { connection_string }) => {
            let accessor = MysqlAccessor::connect(connection_string, None)
                .await
                .map_err(|e| BuiltinError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::Mongo(DatabaseOptionsMongo { connection_string }) => {
            let accessor = MongoAccessor::connect(connection_string)
                .await
                .map_err(|e| BuiltinError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::Snowflake(DatabaseOptionsSnowflake {
            account_name,
            login_name,
            password,
            database_name,
            warehouse,
            role_name,
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
            let accessor = SnowflakeAccessor::connect(conn_params)
                .await
                .map_err(|e| BuiltinError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::Delta(_) => {
            return Err(BuiltinError::Unimplemented("deltalake information listing"))
        }
    };
    Ok(lister)
}

#[derive(Debug, Clone, Copy)]
pub struct GenerateSeries;

#[async_trait]
impl TableFunc for GenerateSeries {
    fn name(&self) -> &str {
        "generate_series"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        // TODO: handle other supported types.
        // - Timestamps
        const PARAMS: &[TableFuncParameters] = &[
            TableFuncParameters {
                params: &[
                    TableFuncParameter {
                        name: "start",
                        typ: DataType::Int64,
                    },
                    TableFuncParameter {
                        name: "stop",
                        typ: DataType::Int64,
                    },
                ],
            },
            TableFuncParameters {
                params: &[
                    TableFuncParameter {
                        name: "start",
                        typ: DataType::Int64,
                    },
                    TableFuncParameter {
                        name: "stop",
                        typ: DataType::Int64,
                    },
                    TableFuncParameter {
                        name: "step",
                        typ: DataType::Int64,
                    },
                ],
            },
            TableFuncParameters {
                params: &[
                    TableFuncParameter {
                        name: "start",
                        typ: DataType::Float64,
                    },
                    TableFuncParameter {
                        name: "stop",
                        typ: DataType::Float64,
                    },
                ],
            },
            TableFuncParameters {
                params: &[
                    TableFuncParameter {
                        name: "start",
                        typ: DataType::Float64,
                    },
                    TableFuncParameter {
                        name: "stop",
                        typ: DataType::Float64,
                    },
                    TableFuncParameter {
                        name: "step",
                        typ: DataType::Float64,
                    },
                ],
            },
        ];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: &[FunctionArg],
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();
                if is_scalar_int(start) && is_scalar_int(stop) {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        start.extract()?,
                        stop.extract()?,
                        1,
                    )
                } else if is_scalar_float(start) && is_scalar_float(stop) {
                    create_straming_table::<GenerateSeriesTypeFloat>(
                        start.extract()?,
                        stop.extract()?,
                        1.0f64,
                    )
                } else {
                    return Err(BuiltinError::UnexpectedArgs {
                        expected: String::from("ints or floats"),
                        scalars: vec![start.clone(), stop.clone()],
                    });
                }
            }
            3 => {
                let mut args = args.iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();
                let step = args.next().unwrap();
                if is_scalar_int(start) && is_scalar_int(stop) && is_scalar_int(step) {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        start.extract()?,
                        stop.extract()?,
                        step.extract()?,
                    )
                } else if is_scalar_float(start) && is_scalar_float(stop) && is_scalar_float(step) {
                    create_straming_table::<GenerateSeriesTypeFloat>(
                        start.extract()?,
                        stop.extract()?,
                        step.extract()?,
                    )
                } else {
                    return Err(BuiltinError::UnexpectedArgs {
                        expected: String::from("ints or floats"),
                        scalars: vec![start.clone(), stop.clone(), step.clone()],
                    });
                }
            }
            _ => return Err(BuiltinError::InvalidNumArgs),
        }
    }
}

fn create_straming_table<T: GenerateSeriesType>(
    start: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
) -> Result<Arc<dyn TableProvider>> {
    if step.is_zero() {
        return Err(BuiltinError::Static("'step' may not be zero"));
    }

    let partition: GenerateSeriesPartition<T> = GenerateSeriesPartition::new(start, stop, step);
    let table = StreamingTable::try_new(partition.schema().clone(), vec![Arc::new(partition)])?;

    Ok(Arc::new(table))
}

trait GenerateSeriesType: Send + Sync + 'static {
    type PrimType: Send + Sync + PartialOrd + AddAssign + Add + Zero + Copy + Unpin;
    const ARROW_TYPE: DataType;

    fn collect_array(batch: Vec<Self::PrimType>) -> Arc<dyn Array>;
}

struct GenerateSeriesTypeInt;

impl GenerateSeriesType for GenerateSeriesTypeInt {
    type PrimType = i64;
    const ARROW_TYPE: DataType = DataType::Int64;

    fn collect_array(series: Vec<i64>) -> Arc<dyn Array> {
        let arr = Int64Array::from_iter_values(series);
        Arc::new(arr)
    }
}

struct GenerateSeriesTypeFloat;

impl GenerateSeriesType for GenerateSeriesTypeFloat {
    type PrimType = f64;
    const ARROW_TYPE: DataType = DataType::Float64;

    fn collect_array(series: Vec<f64>) -> Arc<dyn Array> {
        let arr = Float64Array::from_iter_values(series);
        Arc::new(arr)
    }
}

struct GenerateSeriesPartition<T: GenerateSeriesType> {
    schema: Arc<Schema>,
    start: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
}

impl<T: GenerateSeriesType> GenerateSeriesPartition<T> {
    fn new(start: T::PrimType, stop: T::PrimType, step: T::PrimType) -> Self {
        GenerateSeriesPartition {
            schema: Arc::new(Schema::new([Arc::new(Field::new(
                "generate_series",
                T::ARROW_TYPE,
                false,
            ))])),
            start,
            stop,
            step,
        }
    }
}

impl<T: GenerateSeriesType> PartitionStream for GenerateSeriesPartition<T> {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(GenerateSeriesStream::<T> {
            schema: self.schema.clone(),
            exhausted: false,
            curr: self.start,
            stop: self.stop,
            step: self.step,
        })
    }
}

struct GenerateSeriesStream<T: GenerateSeriesType> {
    schema: Arc<Schema>,
    exhausted: bool,
    curr: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
}

impl<T: GenerateSeriesType> GenerateSeriesStream<T> {
    fn generate_next(&mut self) -> Option<RecordBatch> {
        if self.exhausted {
            return None;
        }

        const BATCH_SIZE: usize = 1000;

        let mut series: Vec<_> = Vec::new();
        if self.curr < self.stop && self.step > T::PrimType::zero() {
            // Going up.
            let mut count = 0;
            while self.curr <= self.stop && count < BATCH_SIZE {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        } else if self.curr > self.stop && self.step < T::PrimType::zero() {
            // Going down.
            let mut count = 0;
            while self.curr >= self.stop && count < BATCH_SIZE {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        }

        if series.len() < BATCH_SIZE {
            self.exhausted = true
        }

        // Calculate the start value for the next iteration.
        if let Some(last) = series.last() {
            self.curr = *last + self.step;
        }

        let arr = T::collect_array(series);
        assert_eq!(arr.data_type(), &T::ARROW_TYPE);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![arr]).unwrap();
        Some(batch)
    }
}

impl<T: GenerateSeriesType> Stream for GenerateSeriesStream<T> {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().generate_next().map(Ok))
    }
}

impl<T: GenerateSeriesType> RecordBatchStream for GenerateSeriesStream<T> {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

fn is_scalar_int(val: &SqlFunctionArg) -> bool {
    use datafusion::sql::sqlparser::ast::FunctionArgExpr;
    match val {
        FunctionArg::Named { .. } => false,
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(v))) => match v {
            SqlValue::Number(s, _) => !s.contains('.'),
            _ => todo!(),
        },
        _ => false,
    }
}

fn is_scalar_float(val: &SqlFunctionArg) -> bool {
    !is_scalar_int(val)
}

pub trait ExtractInto<T> {
    fn extract(&self) -> Result<T>;
}

impl ExtractInto<String> for SqlFunctionArg {
    fn extract(&self) -> Result<String> {
        use datafusion::sql::sqlparser::ast::FunctionArgExpr;
        match self {
            FunctionArg::Named { .. } => todo!(),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(
                SqlValue::UnQuotedString(s),
            ))) => Ok(s.clone()),
            other => Err(BuiltinError::UnexpectedArg {
                scalar: other.clone(),
                expected: DataType::Utf8,
            }),
        }
    }
}

impl ExtractInto<f64> for SqlFunctionArg {
    fn extract(&self) -> Result<f64> {
        use datafusion::sql::sqlparser::ast::FunctionArgExpr;
        match self {
            FunctionArg::Named { .. } => todo!(),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(v))) => match v {
                SqlValue::Number(s, _) => Ok(s.parse::<f64>().unwrap()),
                _ => todo!(),
            },
            other => Err(BuiltinError::UnexpectedArg {
                scalar: other.clone(),
                expected: DataType::Int64,
            }),
        }
    }
}
impl ExtractInto<i64> for SqlFunctionArg {
    fn extract(&self) -> Result<i64> {
        use datafusion::sql::sqlparser::ast::FunctionArgExpr;
        match self {
            FunctionArg::Named { .. } => todo!(),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(v))) => {
                match v {
                    SqlValue::Number(s, _) => {
                        // Check for existence of decimal separator dot
                        if s.contains('.') {
                            Err(BuiltinError::UnexpectedArg {
                                scalar: self.clone(),
                                expected: DataType::Int64,
                            })
                        } else {
                            Ok(s.parse::<i64>().unwrap())
                        }
                    }
                    _ => todo!(),
                }
            }
            other => Err(BuiltinError::UnexpectedArg {
                scalar: other.clone(),
                expected: DataType::Int64,
            }),
        }
    }
}
