use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFuncContextProvider};
use datasources::sqlite::{SqliteAccess, SqliteTableProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadSqlite;

impl ConstBuiltinFunction for ReadSqlite {
    const NAME: &'static str = "read_sqlite";
    const DESCRIPTION: &'static str = "Read a sqlite table";
    const EXAMPLE: &'static str = "SELECT * FROM read_sqlite('/path/to/db.sqlite3', 'table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            2,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ReadSqlite {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Local)
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
                let location: String = args.next().unwrap().try_into()?;
                let table: IdentValue = args.next().unwrap().try_into()?;

                let access = SqliteAccess {
                    db: location.into(),
                };
                let state = access.connect().await.map_err(ExtensionError::access)?;
                let provider = SqliteTableProvider::try_new(state, table)
                    .await
                    .map_err(ExtensionError::access)?;
                Ok(Arc::new(provider))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}
