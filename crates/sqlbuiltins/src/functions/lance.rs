use std::collections::HashMap;
use std::sync::Arc;

use crate::functions::table_location_and_opts;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::lance::scan_lance_table;
use protogen::metastore::types::catalog::RuntimePreference;

/// Function for scanning delta tables.
///
/// Note that this is separate from the other object store functions since
/// initializing object storage happens within the lance lib. We're
/// responsible for providing credentials, then it's responsible for creating
/// the store.
///
/// See <https://github.com/delta-io/delta-rs/issues/1521>
#[derive(Debug, Clone, Copy)]
pub struct LanceScan;

#[async_trait]
impl TableFunc for LanceScan {
    fn runtime_preference(&self) -> RuntimePreference {
        // TODO: Detect runtime.
        RuntimePreference::Remote
    }

    fn name(&self) -> &str {
        "lance_scan"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;
        let dataset = scan_lance_table(&source_url.to_string(), storage_options)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(dataset))
    }
}
