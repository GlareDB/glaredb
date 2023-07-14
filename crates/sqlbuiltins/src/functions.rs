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
use datafusion::{arrow::datatypes::DataType, datasource::TableProvider, scalar::ScalarValue};
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::common::listing::VirtualLister;
use datasources::common::url::{DatasourceUrl, DatasourceUrlScheme};
use datasources::debug::DebugVirtualLister;
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasources::object_store::http::HttpAccessor;
use datasources::object_store::local::{LocalAccessor, LocalTableAccess};
use datasources::object_store::s3::{S3Accessor, S3TableAccess};
use datasources::object_store::{FileType, TableAccessor};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use futures::Stream;
use metastore_client::types::catalog::{CredentialsEntry, DatabaseEntry};
use metastore_client::types::options::{
    CredentialsOptions, DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo,
    DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
};
use num_traits::Zero;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::ops::{Add, AddAssign};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
            Arc::new(PARQUET_SCAN),
            Arc::new(CSV_SCAN),
            Arc::new(JSON_SCAN),
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
    fn get_credentials_entry(&self, name: &str) -> Option<&CredentialsEntry>;
}

/// Value from a function parameter.
#[derive(Debug, Clone)]
pub enum FuncParamValue {
    /// Normalized value from an ident.
    Ident(String),
    /// Scalar value.
    Scalar(ScalarValue),
}

impl fmt::Display for FuncParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) => write!(f, "{s}"),
            Self::Scalar(s) => write!(f, "{s}"),
        }
    }
}

impl FuncParamValue {
    /// Print multiple function parameter values.
    pub fn multiple_to_string(vals: &[Self]) -> String {
        let mut s = String::new();
        write!(&mut s, "(").unwrap();
        let mut sep = "";
        for val in vals {
            write!(&mut s, "{sep}{val}").unwrap();
            sep = ", ";
        }
        write!(&mut s, ")").unwrap();
        s
    }

    /// Wrapper over `FromFuncParamValue::from_param`.
    fn param_into<T>(self) -> Result<T>
    where
        T: FromFuncParamValue,
    {
        T::from_param(self)
    }
}

trait FromFuncParamValue: Sized {
    /// Get the value from parameter.
    fn from_param(value: FuncParamValue) -> Result<Self>;

    /// Check if the value is valid (able to convert).
    fn is_param_valid(value: &FuncParamValue) -> bool;
}

impl FromFuncParamValue for String {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Ident(s) => Ok(s),
            FuncParamValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s),
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::Utf8,
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Ident(_) | FuncParamValue::Scalar(ScalarValue::Utf8(Some(_)))
        )
    }
}

impl FromFuncParamValue for i64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(v as i64),
                ScalarValue::Int16(Some(v)) => Ok(v as i64),
                ScalarValue::Int32(Some(v)) => Ok(v as i64),
                ScalarValue::Int64(Some(v)) => Ok(v),
                ScalarValue::UInt8(Some(v)) => Ok(v as i64),
                ScalarValue::UInt16(Some(v)) => Ok(v as i64),
                ScalarValue::UInt32(Some(v)) => Ok(v as i64),
                ScalarValue::UInt64(Some(v)) => Ok(v as i64), // TODO: Handle overflow?
                other => Err(BuiltinError::UnexpectedArg {
                    param: other.into(),
                    expected: DataType::Int64,
                }),
            },
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::Int64,
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Int8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int64(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt64(Some(_)))
        )
    }
}

impl FromFuncParamValue for f64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(v as f64),
                ScalarValue::Int16(Some(v)) => Ok(v as f64),
                ScalarValue::Int32(Some(v)) => Ok(v as f64),
                ScalarValue::Int64(Some(v)) => Ok(v as f64),
                ScalarValue::UInt8(Some(v)) => Ok(v as f64),
                ScalarValue::UInt16(Some(v)) => Ok(v as f64),
                ScalarValue::UInt32(Some(v)) => Ok(v as f64),
                ScalarValue::UInt64(Some(v)) => Ok(v as f64),
                ScalarValue::Float32(Some(v)) => Ok(v as f64),
                ScalarValue::Float64(Some(v)) => Ok(v),
                other => Err(BuiltinError::UnexpectedArg {
                    param: other.into(),
                    expected: DataType::Float64,
                }),
            },
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::Float64,
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Float32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Float64(Some(_)))
        ) || i64::is_param_valid(value)
    }
}

impl From<String> for FuncParamValue {
    fn from(value: String) -> Self {
        Self::Ident(value)
    }
}

impl From<ScalarValue> for FuncParamValue {
    fn from(value: ScalarValue) -> Self {
        Self::Scalar(value)
    }
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
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str: String = args.next().unwrap().param_into()?;
                let schema: String = args.next().unwrap().param_into()?;
                let table: String = args.next().unwrap().param_into()?;

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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            4 => {
                let mut args = args.into_iter();
                let service_account: String = args.next().unwrap().param_into()?;
                let project_id: String = args.next().unwrap().param_into()?;
                let dataset_id: String = args.next().unwrap().param_into()?;
                let table_id: String = args.next().unwrap().param_into()?;

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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str: String = args.next().unwrap().param_into()?;
                let database: String = args.next().unwrap().param_into()?;
                let collection: String = args.next().unwrap().param_into()?;

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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str: String = args.next().unwrap().param_into()?;
                let schema: String = args.next().unwrap().param_into()?;
                let table: String = args.next().unwrap().param_into()?;

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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            8 => {
                let mut args = args.into_iter();
                let account: String = args.next().unwrap().param_into()?;
                let username: String = args.next().unwrap().param_into()?;
                let password: String = args.next().unwrap().param_into()?;
                let database: String = args.next().unwrap().param_into()?;
                let warehouse: String = args.next().unwrap().param_into()?;
                let role: String = args.next().unwrap().param_into()?;
                let schema: String = args.next().unwrap().param_into()?;
                let table: String = args.next().unwrap().param_into()?;

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

pub const PARQUET_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Parquet, "parquet_scan");

pub const CSV_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Csv, "csv_scan");

pub const JSON_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Json, "ndjson_scan");

#[derive(Debug, Clone, Copy)]
pub struct ObjScanTableFunc(FileType, &'static str);

#[async_trait]
impl TableFunc for ObjScanTableFunc {
    fn name(&self) -> &str {
        let Self(_, name) = self;
        name
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[
            TableFuncParameters {
                params: &[TableFuncParameter {
                    name: "url",
                    typ: DataType::Utf8,
                }],
            },
            TableFuncParameters {
                params: &[
                    TableFuncParameter {
                        name: "url",
                        typ: DataType::Utf8,
                    },
                    TableFuncParameter {
                        name: "credentials",
                        typ: DataType::Utf8,
                    },
                ],
            },
        ];

        PARAMS
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let Self(file_type, _) = self;
        create_provider_for_filetype(ctx, *file_type, args, opts).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;

async fn create_provider_for_filetype(
    ctx: &dyn TableFuncContextProvider,
    file_type: FileType,
    args: Vec<FuncParamValue>,
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Arc<dyn TableProvider>> {
    let store = match args.len() {
        1 => {
            let mut args = args.into_iter();
            let url_string: String = args.next().unwrap().param_into()?;
            let source_url = DatasourceUrl::new(&url_string)?;

            match source_url.scheme() {
                DatasourceUrlScheme::Http => HttpAccessor::try_new(url_string, file_type)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?,
                DatasourceUrlScheme::File => {
                    let location = source_url.path().into_owned();
                    LocalAccessor::new(LocalTableAccess {
                        location,
                        file_type: Some(file_type),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                DatasourceUrlScheme::Gcs => {
                    let service_account_key = opts
                        .remove("service_account_key")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    let bucket_name = source_url
                        .host()
                        .map(|b| b.to_owned())
                        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                    let location = source_url.path().into_owned();

                    GcsAccessor::new(GcsTableAccess {
                        bucket_name,
                        service_acccount_key_json: service_account_key,
                        location,
                        file_type: Some(file_type),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                DatasourceUrlScheme::S3 => {
                    let access_key_id = opts
                        .remove("access_key_id")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    let secret_access_key = opts
                        .remove("secret_access_key")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    let bucket_name = source_url
                        .host()
                        .map(|b| b.to_owned())
                        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                    let location = source_url.path().into_owned();

                    // S3 requires a region parameter.
                    const REGION_KEY: &str = "region";
                    let region = opts
                        .remove(REGION_KEY)
                        .ok_or(BuiltinError::MissingNamedArgument(REGION_KEY))?
                        .param_into()?;

                    S3Accessor::new(S3TableAccess {
                        bucket_name,
                        location,
                        file_type: Some(file_type),
                        region,
                        access_key_id,
                        secret_access_key,
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
            }
        }
        2 => {
            let mut args = args.into_iter();
            let url_string: String = args.next().unwrap().param_into()?;
            let source_url = DatasourceUrl::new(url_string)?;

            let creds: String = args.next().unwrap().param_into()?;
            let creds = ctx
                .get_credentials_entry(&creds)
                .ok_or(BuiltinError::Static("missing credentials object"))?;

            match source_url.scheme() {
                DatasourceUrlScheme::Gcs => {
                    let service_account_key = match &creds.options {
                        CredentialsOptions::Gcp(o) => o.service_account_key.to_owned(),
                        _ => return Err(BuiltinError::Static("invalid credentials for GCS")),
                    };

                    let bucket_name = source_url
                        .host()
                        .map(|b| b.to_owned())
                        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                    let location = source_url.path().into_owned();

                    GcsAccessor::new(GcsTableAccess {
                        bucket_name,
                        service_acccount_key_json: Some(service_account_key),
                        location,
                        file_type: Some(file_type),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                DatasourceUrlScheme::S3 => {
                    let (access_key_id, secret_access_key) = match &creds.options {
                        CredentialsOptions::Aws(o) => {
                            (o.access_key_id.to_owned(), o.secret_access_key.to_owned())
                        }
                        _ => return Err(BuiltinError::Static("invalid credentials for GCS")),
                    };

                    let bucket_name = source_url
                        .host()
                        .map(|b| b.to_owned())
                        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                    let location = source_url.path().into_owned();

                    // S3 requires a region parameter.
                    const REGION_KEY: &str = "region";
                    let region = opts
                        .remove(REGION_KEY)
                        .ok_or(BuiltinError::MissingNamedArgument(REGION_KEY))?
                        .param_into()?;

                    S3Accessor::new(S3TableAccess {
                        bucket_name,
                        location,
                        file_type: Some(file_type),
                        region,
                        access_key_id: Some(access_key_id),
                        secret_access_key: Some(secret_access_key),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                _ => {
                    return Err(BuiltinError::Static(
                        "Unsupported datasource URL for given parameters",
                    ))
                }
            }
        }
        _ => return Err(BuiltinError::InvalidNumArgs),
    };
    Ok(store)
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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            1 => {
                let mut args = args.into_iter();
                let database: String = args.next().unwrap().param_into()?;

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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.into_iter();
                let database: String = args.next().unwrap().param_into()?;
                let schema_name: String = args.next().unwrap().param_into()?;

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
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();

                if i64::is_param_valid(&start) && i64::is_param_valid(&stop) {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        start.param_into()?,
                        stop.param_into()?,
                        1,
                    )
                } else if f64::is_param_valid(&start) && f64::is_param_valid(&stop) {
                    create_straming_table::<GenerateSeriesTypeFloat>(
                        start.param_into()?,
                        stop.param_into()?,
                        1.0_f64,
                    )
                } else {
                    return Err(BuiltinError::UnexpectedArgs {
                        expected: String::from("ints or floats"),
                        params: vec![start, stop],
                    });
                }
            }
            3 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();
                let step = args.next().unwrap();

                if i64::is_param_valid(&start)
                    && i64::is_param_valid(&stop)
                    && i64::is_param_valid(&step)
                {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        start.param_into()?,
                        stop.param_into()?,
                        step.param_into()?,
                    )
                } else if f64::is_param_valid(&start)
                    && f64::is_param_valid(&stop)
                    && f64::is_param_valid(&step)
                {
                    create_straming_table::<GenerateSeriesTypeFloat>(
                        start.param_into()?,
                        stop.param_into()?,
                        step.param_into()?,
                    )
                } else {
                    return Err(BuiltinError::UnexpectedArgs {
                        expected: String::from("ints or floats"),
                        params: vec![start, stop, step],
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
