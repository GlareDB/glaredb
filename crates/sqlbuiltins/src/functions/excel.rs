use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::excel::read_excel_impl;
use protogen::metastore::types::catalog::RuntimePreference;

use super::table_location_and_opts;

#[derive(Debug, Clone, Copy)]
pub struct ExcelScan;

#[async_trait]
impl TableFunc for ExcelScan {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Local
    }

    fn name(&self) -> &str {
        "read_excel"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, options) = table_location_and_opts(ctx, args, &mut opts)?;
        let url = PathBuf::from(source_url.to_string());
        let url = resolve_homedir(&url);
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

pub fn resolve_homedir(path: &Path) -> PathBuf {
    if path.starts_with("~") {
        if let Some(homedir) = home::home_dir() {
            return homedir.join(path.strip_prefix("~").unwrap());
        }
    }
    path.into()
}
