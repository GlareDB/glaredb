use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FuncParamValue, IdentValue, TableFuncContextProvider, VirtualLister,
};
use datasources::bigquery::BigQueryAccessor;
use datasources::debug::DebugVirtualLister;
use datasources::mongodb::MongoDbAccessor;
use datasources::mysql::MysqlAccessor;
use datasources::postgres::PostgresAccess;
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection};
use datasources::sqlserver::SqlServerAccess;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};
use protogen::metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongoDb, DatabaseOptionsMysql,
    DatabaseOptionsPostgres, DatabaseOptionsSnowflake, DatabaseOptionsSqlServer,
};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;
impl ConstBuiltinFunction for ListSchemas {
    const NAME: &'static str = "list_schemas";
    const DESCRIPTION: &'static str = "Lists schemas in a database";
    const EXAMPLE: &'static str = "SELECT * FROM list_schemas('database')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for ListSchemas {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // Currently all of our db's are "external" so it'd never be preferred
        // to run this locally.
        Ok(RuntimePreference::Remote)
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
                let database: IdentValue = args.next().unwrap().try_into()?;

                let fields = vec![Field::new("schema_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_virtual_lister_from_context(ctx, database.into()).await?;
                let schema_list = lister
                    .list_schemas()
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
                let schema_list: StringArray = schema_list.into_iter().map(Some).collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(schema_list)])
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListTables;

impl ConstBuiltinFunction for ListTables {
    const NAME: &'static str = "list_tables";
    const DESCRIPTION: &'static str = "Lists tables in a schema";
    const EXAMPLE: &'static str = "SELECT * FROM list_tables('database', 'schema')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            3,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ListTables {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // Currently all of our db's are "external" so it'd never be preferred
        // to run this locally.
        Ok(RuntimePreference::Remote)
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
                let database: IdentValue = args.next().unwrap().try_into()?;
                let schema_name: IdentValue = args.next().unwrap().try_into()?;

                let fields = vec![Field::new("table_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_virtual_lister_from_context(ctx, database.into()).await?;
                let tables_list = lister
                    .list_tables(schema_name.as_str())
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
                let tables_list: StringArray = tables_list.into_iter().map(Some).collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(tables_list)])
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListColumns;

impl ConstBuiltinFunction for ListColumns {
    const NAME: &'static str = "list_columns";
    const DESCRIPTION: &'static str = "Lists columns in a table";
    const EXAMPLE: &'static str = "SELECT * FROM list_columns('database', 'schema', 'table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            3,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ListColumns {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // Currently all of our db's are "external" so it'd never be preferred
        // to run this locally.
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let database: IdentValue = args.next().unwrap().try_into()?;
                let schema_name: IdentValue = args.next().unwrap().try_into()?;
                let table_name: IdentValue = args.next().unwrap().try_into()?;

                let fields = vec![
                    Field::new("column_name", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                    Field::new("nullable", DataType::Boolean, false),
                ];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_virtual_lister_from_context(ctx, database.into()).await?;
                let columns_list = lister
                    .list_columns(schema_name.as_str(), table_name.as_str())
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                let (mut column_name_list, (mut data_type_list, mut nullable_list)): (
                    StringBuilder,
                    (StringBuilder, BooleanBuilder),
                ) = columns_list
                    .into_iter()
                    .map(|field| {
                        (
                            Some(field.name().to_owned()),
                            (
                                Some(field.data_type().to_string()),
                                Some(field.is_nullable()),
                            ),
                        )
                    })
                    .unzip();

                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(column_name_list.finish()),
                        Arc::new(data_type_list.finish()),
                        Arc::new(nullable_list.finish()),
                    ],
                )
                .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}

/// Get a virtual lister by looking up the database entry from the session
/// catalog.
async fn get_virtual_lister_from_context(
    ctx: &dyn TableFuncContextProvider,
    dbname: String,
) -> Result<Box<dyn VirtualLister + '_>> {
    let db = ctx.get_session_catalog().resolve_database(&dbname).ok_or(
        ExtensionError::MissingObject {
            obj_typ: "database",
            name: dbname,
        },
    )?;

    let lister = get_virtual_lister_for_db(ctx, &db.options).await?;
    Ok(lister)
}

/// Get a lister for a database (including internal).
///
/// Lifetime annotations just indicate that the returned lister has a shorter
/// lifetime bound than everything else. This is needed since lister may be for
/// the session catalog (bounded to the lifetime of the context) _or_ we create
/// a client for an external database (unbounded lifetime).
pub(crate) async fn get_virtual_lister_for_db<'a, 'b: 'a, 'c: 'b>(
    ctx: &'c dyn TableFuncContextProvider,
    opts: &'b DatabaseOptions,
) -> Result<Box<dyn VirtualLister + 'a>> {
    match opts {
        DatabaseOptions::Internal(_) => Ok(ctx.get_catalog_lister()),
        other => get_virtual_lister_for_external_db(other).await,
    }
}

/// Gets a lister for an external database using the provided options.
///
/// Will panic if attempting to get a lister for an internal database.
pub(crate) async fn get_virtual_lister_for_external_db(
    opts: &DatabaseOptions,
) -> Result<Box<dyn VirtualLister>> {
    let lister: Box<dyn VirtualLister> = match opts {
        DatabaseOptions::Internal(_) => panic!("attempted to get lister for internal db"),
        DatabaseOptions::Debug(_) => Box::new(DebugVirtualLister),
        DatabaseOptions::Postgres(DatabaseOptionsPostgres { connection_string }) => {
            // TODO: We're not using the configured tunnel?
            let access = PostgresAccess::new_from_conn_str(connection_string.clone(), None);
            let state = access
                .connect()
                .await
                .map_err(|e| ExtensionError::Access(Box::new(e)))?;
            Box::new(state)
        }
        DatabaseOptions::BigQuery(DatabaseOptionsBigQuery {
            service_account_key,
            project_id,
        }) => {
            let accessor =
                BigQueryAccessor::connect(service_account_key.clone(), project_id.clone())
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::Mysql(DatabaseOptionsMysql { connection_string }) => {
            let accessor = MysqlAccessor::connect(connection_string, None)
                .await
                .map_err(|e| ExtensionError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::MongoDb(DatabaseOptionsMongoDb { connection_string }) => {
            let accessor = MongoDbAccessor::connect(connection_string)
                .await
                .map_err(|e| ExtensionError::Access(Box::new(e)))?;
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
                .map_err(|e| ExtensionError::Access(Box::new(e)))?;
            Box::new(accessor)
        }
        DatabaseOptions::SqlServer(DatabaseOptionsSqlServer { connection_string }) => {
            let access = SqlServerAccess::try_new_from_ado_string(connection_string)
                .map_err(ExtensionError::access)?;
            let state = access.connect().await.map_err(ExtensionError::access)?;
            Box::new(state)
        }
        DatabaseOptions::Clickhouse(_) => {
            return Err(ExtensionError::Unimplemented(
                "Clickhouse information listing",
            ))
        }
        DatabaseOptions::Delta(_) => {
            return Err(ExtensionError::Unimplemented(
                "deltalake information listing",
            ))
        }
    };
    Ok(lister)
}
