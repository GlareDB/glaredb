//! Builtin table returning functions.
use crate::errors::{BuiltinError, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
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
use datasources::debug::DebugVirtualLister;
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::http::HttpAccessor;
use datasources::object_store::parquet::ParquetTableProvider;
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use futures::Stream;
use metastoreproto::types::catalog::DatabaseEntry;
use metastoreproto::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo, DatabaseOptionsMysql,
    DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
};
use once_cell::sync::Lazy;
use std::collections::HashMap;
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
            Arc::new(ParquetScan),
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
        args: Vec<ScalarValue>,
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
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
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

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
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

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
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

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
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

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
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
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            // parquet_scan(url)
            1 => {
                let mut args = args.into_iter();
                let url = string_from_scalar(args.next().unwrap())?;
                let accessor = HttpAccessor::try_new(url)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = ParquetTableProvider::from_table_accessor(accessor, false)
                    .await
                    .unwrap();

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::Unimplemented("invalid number of arguments")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;

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
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            1 => {
                let mut args = args.into_iter();
                let database = string_from_scalar(args.next().unwrap())?;

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
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.into_iter();
                let database = string_from_scalar(args.next().unwrap())?;
                let schema_name = string_from_scalar(args.next().unwrap())?;

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
        // - Floats
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
        ];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (start, stop, step) = match args.len() {
            2 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();
                (i64_from_scalar(start)?, i64_from_scalar(stop)?, 1)
            }
            3 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();
                let step = args.next().unwrap();
                (
                    i64_from_scalar(start)?,
                    i64_from_scalar(stop)?,
                    i64_from_scalar(step)?,
                )
            }
            _ => return Err(BuiltinError::InvalidNumArgs),
        };

        if step == 0 {
            return Err(BuiltinError::Static("'step' may not be zero"));
        }

        let partition = GenerateSeriesIntPartition::new(start, stop, step);
        let table = StreamingTable::try_new(partition.schema().clone(), vec![Arc::new(partition)])?;

        Ok(Arc::new(table))
    }
}

struct GenerateSeriesIntPartition {
    schema: Arc<Schema>,
    start: i64,
    stop: i64,
    step: i64,
}

impl GenerateSeriesIntPartition {
    fn new(start: i64, stop: i64, step: i64) -> Self {
        GenerateSeriesIntPartition {
            schema: Arc::new(Schema::new([Arc::new(Field::new(
                "generate_series",
                DataType::Int64,
                false,
            ))])),
            start,
            stop,
            step,
        }
    }
}

impl PartitionStream for GenerateSeriesIntPartition {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(GenerateSeriesIntStream {
            schema: self.schema.clone(),
            exhausted: false,
            curr: self.start,
            stop: self.stop,
            step: self.step,
        })
    }
}

struct GenerateSeriesIntStream {
    schema: Arc<Schema>,
    exhausted: bool,
    curr: i64,
    stop: i64,
    step: i64,
}

impl GenerateSeriesIntStream {
    fn generate_next(&mut self) -> Option<RecordBatch> {
        if self.exhausted {
            return None;
        }

        const BATCH_SIZE: usize = 1000;

        let series: Vec<_> = if self.curr < self.stop && self.step > 0 {
            // Going up.
            (self.curr..=self.stop)
                .step_by(self.step as usize)
                .take(BATCH_SIZE)
                .collect()
        } else if self.curr > self.stop && self.step < 0 {
            // Going down.
            (self.stop..=self.curr)
                .rev()
                .step_by(self.step.unsigned_abs() as usize)
                .take(BATCH_SIZE)
                .collect()
        } else {
            // Zero rows.
            std::iter::empty().collect()
        };

        if series.len() < BATCH_SIZE {
            self.exhausted = true
        }

        // Calculate the start value for the next iteration.
        if let Some(last) = series.last() {
            self.curr = *last + self.step;
        }

        let arr = Int64Array::from_iter_values(series);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)]).unwrap();

        Some(batch)
    }
}

impl Stream for GenerateSeriesIntStream {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.generate_next().map(Ok))
    }
}

impl RecordBatchStream for GenerateSeriesIntStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

fn i64_from_scalar(val: ScalarValue) -> Result<i64> {
    match val {
        ScalarValue::Int8(Some(v)) => Ok(v as i64),
        ScalarValue::Int16(Some(v)) => Ok(v as i64),
        ScalarValue::Int32(Some(v)) => Ok(v as i64),
        ScalarValue::Int64(Some(v)) => Ok(v),
        ScalarValue::UInt8(Some(v)) => Ok(v as i64),
        ScalarValue::UInt16(Some(v)) => Ok(v as i64),
        ScalarValue::UInt32(Some(v)) => Ok(v as i64),
        ScalarValue::UInt64(Some(v)) => Ok(v as i64), // TODO: Handle overflow?
        other => Err(BuiltinError::UnexpectedArg {
            scalar: other,
            expected: DataType::Int64,
        }),
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
