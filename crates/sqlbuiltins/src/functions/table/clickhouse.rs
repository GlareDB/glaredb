use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Signature;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadClickhouse;

impl ConstBuiltinFunction for ReadClickhouse {
    const NAME: &'static str = "read_clickhouse";
    const DESCRIPTION: &'static str = "Reads a Clickhouse table";
    const EXAMPLE: &'static str = "HIIIIIIIIIIIIII"; // TODO
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    fn signature(&self) -> Option<Signature> {
        None // TODO
    }
}

#[async_trait]
impl TableFunc for ReadClickhouse {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
    }
    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        unimplemented!()
    }
}
