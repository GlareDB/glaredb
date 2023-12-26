use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;

use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::bson::table::bson_streaming_table;
use datasources::object_store::generic::GenericStoreAccess;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::functions::table::{table_location_and_opts, TableFunc};
use crate::functions::{ConstBuiltinFunction, FunctionType};

#[derive(Debug, Clone, Copy, Default)]
pub struct BsonScan;

impl ConstBuiltinFunction for BsonScan {
    const NAME: &'static str = "read_bson";
    const DESCRIPTION: &'static str = "Reads one or more bson files. Supports globbing.";
    const EXAMPLE: &'static str = "SELECT * FROM bson_scan('file:///path/to/table*.bson')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for BsonScan {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Unspecified
    }

    // TODO: most of this should be implemented as a TableProvider in
    // the datasources bson package and just wrapped here.
    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>, ExtensionError> {
        // parse arguments to see how many documents to consider for schema inheritance.
        let sample_size = match opts.get("schema_sample_size") {
            // TODO: set a maximum (1024?) or have an adaptive mode
            // (at least n but stop after n the same) or skip documents
            Some(v) => v.to_owned().try_into()?,
            None => 100,
        };

        // setup storage access
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let store_access = GenericStoreAccess::new_from_location_and_opts(
            source_url.to_string().as_str(),
            storage_options,
        )
        .map_err(|e| ExtensionError::Arrow(e.into()))?;

        bson_streaming_table(store_access, Some(sample_size), source_url).await
    }
}
