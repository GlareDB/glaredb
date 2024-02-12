use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::flightsql::{FlightSqlSourceConnectionOptions, FlightSqlSourceProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadFlightSql;

impl ConstBuiltinFunction for ReadFlightSql {
    const NAME: &'static str = "read_flightsql";
    const DESCRIPTION: &'static str = "Reads a FlightSQL table";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_flightsql('https://localhost:37019', 'database', 'token')";
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
impl TableFunc for ReadFlightSql {
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
        if args.len() != 4 {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let mut args = args.into_iter();
        let uri: String = args.next().unwrap().try_into()?;
        let database: String = args.next().unwrap().try_into()?;
        let table: String = args.next().unwrap().try_into()?;
        let token: String = args.next().unwrap().try_into()?;

        let opts = FlightSqlSourceConnectionOptions {
            uri,
            database,
            token,
            table,
        };

        Ok(Arc::new(FlightSqlSourceProvider::try_new(opts).await?))
    }
}
