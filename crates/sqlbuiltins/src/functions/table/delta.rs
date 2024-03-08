use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::lake::delta::access::load_table_direct;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::{table_location_and_opts, TableFunc};
use crate::functions::ConstBuiltinFunction;

/// Function for scanning delta tables.
///
/// Note that this is separate from the other object store functions since
/// initializing object storage happens within the deltalake lib. We're
/// responsible for providing credentials, then it's responsible for creating
/// the store.
///
/// See <https://github.com/delta-io/delta-rs/issues/1521>
#[derive(Debug, Clone, Copy)]
pub struct DeltaScan;

impl ConstBuiltinFunction for DeltaScan {
    const NAME: &'static str = "delta_scan";
    const DESCRIPTION: &'static str = "Scans a delta table";
    const EXAMPLE: &'static str = "SELECT * FROM delta_scan('file:///path/to/table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    const ALIASES: &'static [&'static str] = &["read_delta"];
}

#[async_trait]
impl TableFunc for DeltaScan {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // TODO: Detect runtime.
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let table = load_table_direct(&source_url.to_string(), storage_options)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(table))
    }
}
