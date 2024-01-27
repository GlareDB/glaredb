use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::json::table::json_streaming_table;
use datasources::object_store::generic::GenericStoreAccess;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::functions::table::{table_location_and_opts, TableFunc};
use crate::functions::{ConstBuiltinFunction, FunctionType};

#[derive(Debug, Clone, Copy, Default)]
pub struct JsonScan;

impl ConstBuiltinFunction for JsonScan {
    const NAME: &'static str = "read_json";
    const DESCRIPTION: &'static str = "Reads one or more JSON files or URLs. Supports globbing.";
    const EXAMPLE: &'static str = "SELECT * FROM read_json('https://example.com/feed.json')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for JsonScan {
    fn detect_runtime(
        &self,
        args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference, ExtensionError> {
        if let Some(arg) = args.first() {
            let url: String = arg.clone().try_into()?;
            let source_url =
                DatasourceUrl::try_new(url).map_err(|e| ExtensionError::Access(Box::new(e)))?;
            Ok(match source_url.datasource_url_type() {
                DatasourceUrlType::File => RuntimePreference::Local,
                _ => RuntimePreference::Remote,
            })
        } else {
            Err(ExtensionError::ExpectedIndexedArgument {
                index: 0,
                what: "location of the table".to_string(),
            })
        }
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>, ExtensionError> {
        // setup storage access
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let store_access = GenericStoreAccess::new_from_location_and_opts(
            source_url.to_string().as_str(),
            storage_options,
        )?;

        Ok(json_streaming_table(store_access, source_url).await?)
    }
}
