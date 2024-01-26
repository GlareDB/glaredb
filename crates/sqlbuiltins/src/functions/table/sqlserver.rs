use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::sqlserver::{
    SqlServerAccess,
    SqlServerTableProvider,
    SqlServerTableProviderConfig,
};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadSqlServer;

impl ConstBuiltinFunction for ReadSqlServer {
    const NAME: &'static str = "read_sqlserver";
    const DESCRIPTION: &'static str = "Reads an SQL Server table";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_sqlserver('server=tcp:localhost,1433;user=SA;password=Password123;TrustServerCertificate=true', 'dbo', 'table')";
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
impl TableFunc for ReadSqlServer {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Remote)
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
                let conn_str: String = args.next().unwrap().try_into()?;
                let schema: String = args.next().unwrap().try_into()?;
                let table: String = args.next().unwrap().try_into()?;

                let access = SqlServerAccess::try_new_from_ado_string(&conn_str)
                    .map_err(ExtensionError::access)?;
                let prov_conf = SqlServerTableProviderConfig {
                    access,
                    schema,
                    table,
                };
                let prov = SqlServerTableProvider::try_new(prov_conf)
                    .await
                    .map_err(ExtensionError::access)?;

                Ok(Arc::new(prov))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}
