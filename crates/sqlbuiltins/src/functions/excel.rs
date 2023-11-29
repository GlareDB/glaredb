use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrl;
use datasources::excel::read_excel_impl;
use ioutil::resolve_path;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::builtins::{BuiltinFunction, TableFunc};

use super::table_location_and_opts;

#[derive(Debug, Clone, Copy)]
pub struct ExcelScan;

impl BuiltinFunction for ExcelScan {
    fn name(&self) -> &str {
        "read_excel"
    }
}

#[async_trait]
impl TableFunc for ExcelScan {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Local
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, options) = table_location_and_opts(ctx, args, &mut opts)?;

        let url = match source_url {
            DatasourceUrl::File(path) => path,
            DatasourceUrl::Url(url) => {
                return Err(ExtensionError::String(format!(
                    "Expected file, received url: {}",
                    url
                )))
            }
        };

        let url = resolve_path(&url)?;
        let sheet_name = options.inner.get("sheet_name").map(|v| v.as_str());
        let has_header = options
            .inner
            .get("has_header")
            .and_then(|v| v.as_str().parse::<bool>().ok());

        let infer_schema_len = options
            .inner
            .get("infer_rows")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100);

        let table = read_excel_impl(&url, sheet_name, has_header, infer_schema_len)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;
        Ok(Arc::new(table))
    }
}
