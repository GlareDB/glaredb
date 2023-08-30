use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider};
use datasources::bigquery::BigQueryAccessor;
use datasources::common::listing::VirtualLister;
use datasources::debug::DebugVirtualLister;
use datasources::mongodb::MongoAccessor;
use datasources::mysql::MysqlAccessor;
use datasources::postgres::PostgresAccess;
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection};
use protogen::metastore::types::catalog::RuntimePreference;
use protogen::metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo, DatabaseOptionsMysql,
    DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
};

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;

#[async_trait]
impl TableFunc for ListSchemas {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Unspecified
    }
    fn name(&self) -> &str {
        "list_schemas"
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
                let database: IdentValue = args.next().unwrap().param_into()?;

                let fields = vec![Field::new("schema_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_db_lister(ctx, database.into()).await?;
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

#[async_trait]
impl TableFunc for ListTables {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Unspecified
    }
    fn name(&self) -> &str {
        "list_tables"
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
                let database: IdentValue = args.next().unwrap().param_into()?;
                let schema_name: IdentValue = args.next().unwrap().param_into()?;

                let fields = vec![Field::new("table_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_db_lister(ctx, database.into()).await?;
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

async fn get_db_lister(
    ctx: &dyn TableFuncContextProvider,
    dbname: String,
) -> Result<Box<dyn VirtualLister>> {
    let db = ctx
        .get_database_entry(&dbname)
        .ok_or(ExtensionError::MissingObject {
            obj_typ: "database",
            name: dbname,
        })?;

    let lister: Box<dyn VirtualLister> = match &db.options {
        DatabaseOptions::Internal(_) => unimplemented!(), // TODO: https://github.com/GlareDB/glaredb/issues/1153
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
        DatabaseOptions::Mongo(DatabaseOptionsMongo { connection_string }) => {
            let accessor = MongoAccessor::connect(connection_string)
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
        DatabaseOptions::Delta(_) => {
            return Err(ExtensionError::Unimplemented(
                "deltalake information listing",
            ))
        }
    };
    Ok(lister)
}
