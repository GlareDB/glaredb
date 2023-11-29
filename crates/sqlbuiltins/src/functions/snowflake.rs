use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use protogen::metastore::types::catalog::RuntimePreference;

use crate::builtins::{BuiltinFunction, TableFunc};

#[derive(Debug, Clone, Copy)]
pub struct ReadSnowflake;

impl BuiltinFunction for ReadSnowflake {
    fn name(&self) -> &str {
        "read_snowflake"
    }
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            8,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ReadSnowflake {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
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
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
                let prov = accessor
                    .into_table_provider(access_info, true)
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}
