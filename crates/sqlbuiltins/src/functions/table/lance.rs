use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrlType;
use datasources::lance::LanceTable;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::object_store::urls_from_args;
use super::{table_location_and_opts, TableFunc};
use crate::functions::ConstBuiltinFunction;

/// Function for scanning delta tables.
///
/// Note that this is separate from the other object store functions since
/// initializing object storage happens within the lance lib. We're
/// responsible for providing credentials, then it's responsible for creating
/// the store.
#[derive(Debug, Clone, Copy)]
pub struct LanceScan;

impl ConstBuiltinFunction for LanceScan {
    const NAME: &'static str = "lance_scan";
    const DESCRIPTION: &'static str = "Scans a Lance table";
    const EXAMPLE: &'static str = "SELECT * FROM lance_scan('file:///path/to/table.lance')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for LanceScan {
    fn detect_runtime(
        &self,
        args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        let urls = urls_from_args(args)?;
        // All urls are of the same type, just need to get the runtime from the
        // first.
        Ok(match urls.first().unwrap().datasource_url_type() {
            DatasourceUrlType::File => RuntimePreference::Local,
            DatasourceUrlType::Http => RuntimePreference::Remote,
            DatasourceUrlType::Gcs => RuntimePreference::Remote,
            DatasourceUrlType::S3 => RuntimePreference::Remote,
            DatasourceUrlType::Azure => RuntimePreference::Remote,
        })
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;
        Ok(Arc::new(
            LanceTable::new(&source_url.to_string(), storage_options)
                .await
                .map_err(|e| ExtensionError::Access(Box::new(e)))?,
        ))
    }
}
