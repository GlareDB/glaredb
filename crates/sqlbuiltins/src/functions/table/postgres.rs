use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::postgres::{PostgresAccess, PostgresTableProvider, PostgresTableProviderConfig};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadPostgres;

impl ConstBuiltinFunction for ReadPostgres {
    const NAME: &'static str = "read_postgres";
    const DESCRIPTION: &'static str = "Reads a Postgres table";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_postgres('postgres://localhost:5432', 'database', 'table')";
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
impl TableFunc for ReadPostgres {
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
            3 => {
                let mut args = args.into_iter();
                let conn_str: String = args.next().unwrap().param_into()?;
                let schema: String = args.next().unwrap().param_into()?;
                let table: String = args.next().unwrap().param_into()?;

                let access = PostgresAccess::new_from_conn_str(conn_str, None);
                let prov_conf = PostgresTableProviderConfig {
                    access,
                    schema,
                    table,
                };
                let prov = PostgresTableProvider::try_new(prov_conf)
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}
