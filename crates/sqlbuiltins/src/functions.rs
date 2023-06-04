//! Builtin table returning functions.
use crate::errors::{BuiltinError, Result};
use async_trait::async_trait;
use datafusion::{arrow::datatypes::DataType, datasource::TableProvider, scalar::ScalarValue};
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

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
            Arc::new(ReadPostgres),
            Arc::new(ReadBigQuery),
            Arc::new(ReadMongoDb),
            Arc::new(ReadMysql),
            Arc::new(ReadSnowflake),
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
    async fn create_provider(&self, args: Vec<ScalarValue>) -> Result<Arc<dyn TableProvider>>;
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

    async fn create_provider(&self, args: Vec<ScalarValue>) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str = string_from_scalar(args.next().unwrap())?;
                let schema = string_from_scalar(args.next().unwrap())?;
                let table = string_from_scalar(args.next().unwrap())?;

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

    async fn create_provider(&self, args: Vec<ScalarValue>) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            4 => {
                let mut args = args.into_iter();
                let service_account = string_from_scalar(args.next().unwrap())?;
                let project_id = string_from_scalar(args.next().unwrap())?;
                let dataset_id = string_from_scalar(args.next().unwrap())?;
                let table_id = string_from_scalar(args.next().unwrap())?;

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

    async fn create_provider(&self, args: Vec<ScalarValue>) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str = string_from_scalar(args.next().unwrap())?;
                let database = string_from_scalar(args.next().unwrap())?;
                let collection = string_from_scalar(args.next().unwrap())?;

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

    async fn create_provider(&self, args: Vec<ScalarValue>) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str = string_from_scalar(args.next().unwrap())?;
                let schema = string_from_scalar(args.next().unwrap())?;
                let table = string_from_scalar(args.next().unwrap())?;

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

    async fn create_provider(&self, args: Vec<ScalarValue>) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            8 => {
                let mut args = args.into_iter();
                let account = string_from_scalar(args.next().unwrap())?;
                let username = string_from_scalar(args.next().unwrap())?;
                let password = string_from_scalar(args.next().unwrap())?;
                let database = string_from_scalar(args.next().unwrap())?;
                let warehouse = string_from_scalar(args.next().unwrap())?;
                let role = string_from_scalar(args.next().unwrap())?;
                let schema = string_from_scalar(args.next().unwrap())?;
                let table = string_from_scalar(args.next().unwrap())?;

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

fn string_from_scalar(val: ScalarValue) -> Result<String> {
    match val {
        ScalarValue::Utf8(Some(s)) => Ok(s),
        other => Err(BuiltinError::UnexpectedArg {
            scalar: other,
            expected: DataType::Utf8,
        }),
    }
}
