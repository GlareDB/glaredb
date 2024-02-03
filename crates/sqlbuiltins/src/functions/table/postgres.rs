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
    const DESCRIPTION: &'static str = "Read a Postgres table";
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
        if args.len() < 3 || args.len() > 4 {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let mut args = args.into_iter();
        let conn_str: String = args.next().unwrap().try_into()?;
        let schema: String = args.next().unwrap().try_into()?;
        let table: String = args.next().unwrap().try_into()?;

        let access = PostgresAccess::new_from_conn_str(conn_str, None);

        let order_by: Option<String> = args.next().map(|p| p.try_into().ok()).flatten();

        let prov_conf = PostgresTableProviderConfig {
            access,
            schema,
            table,
            order_by,
        };
        let prov = PostgresTableProvider::try_new(prov_conf)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(prov))
    }
}
