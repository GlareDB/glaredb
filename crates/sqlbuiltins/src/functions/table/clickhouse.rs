use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::clickhouse::{ClickhouseAccess, ClickhouseTableProvider, OwnedClickhouseTableRef};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadClickhouse;

impl ConstBuiltinFunction for ReadClickhouse {
    const NAME: &'static str = "read_clickhouse";
    const DESCRIPTION: &'static str = "Read a Clickhouse table";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_clickhouse('clickhouse://user:password@localhost:9000/database', 'table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::one_of(
            vec![
                TypeSignature::Uniform(2, vec![DataType::Utf8]),
                TypeSignature::Uniform(3, vec![DataType::Utf8]),
            ],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ReadClickhouse {
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
        let total_args = args.len();
        let mut args = args.into_iter();
        let conn_string: String = args.next().unwrap().try_into()?;

        let table_ref = match total_args {
            2 => {
                let schema: Option<String> = None;
                let table: String = args.next().unwrap().try_into()?;
                OwnedClickhouseTableRef::new(schema, table)
            }
            3 => {
                let schema: String = args.next().unwrap().try_into()?;
                let table: String = args.next().unwrap().try_into()?;
                OwnedClickhouseTableRef::new(Some(schema), table)
            }
            _ => return Err(ExtensionError::InvalidNumArgs),
        };

        let access = ClickhouseAccess::new_from_connection_string(conn_string);

        let prov = ClickhouseTableProvider::try_new(access, table_ref)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(prov))
    }
}
