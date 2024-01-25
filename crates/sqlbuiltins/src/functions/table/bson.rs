use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::bson::table::bson_streaming_table;
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::object_store::generic::GenericStoreAccess;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::functions::table::{table_location_and_opts, TableFunc};
use crate::functions::{ConstBuiltinFunction, FunctionType};

#[derive(Debug, Clone, Copy, Default)]
pub struct BsonScan;

impl ConstBuiltinFunction for BsonScan {
    const NAME: &'static str = "read_bson";
    const DESCRIPTION: &'static str = "Reads one or more BSON files. Supports globbing.";
    const EXAMPLE: &'static str = "SELECT * FROM read_bson('./path/to/table*.bson')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for BsonScan {
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
        )?;

        Ok(bson_streaming_table(store_access, Some(sample_size), source_url).await?)
    }
}
