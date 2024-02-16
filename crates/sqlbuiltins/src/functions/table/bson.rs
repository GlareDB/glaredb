use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::bson::table::bson_streaming_table;
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::object_store::generic::GenericStoreAccess;
use ioutil::resolve_path;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::functions::table::object_store::urls_from_args;
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
        let urls = urls_from_args(args)?;
        if urls.is_empty() {
            return Err(ExtensionError::ExpectedIndexedArgument {
                index: 0,
                what: "location of the table".to_string(),
            });
        }

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
    ) -> Result<Arc<dyn TableProvider>, ExtensionError> {
        // parse arguments to see how many documents to consider for schema inheritance.
        let sample_size = match opts.get("schema_sample_size") {
            // TODO: set a maximum (1024?) or have an adaptive mode
            // (at least n but stop after n the same) or skip documents
            Some(v) => v.to_owned().try_into()?,
            None => 100,
        };

        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let url = match source_url {
            DatasourceUrl::File(path) => DatasourceUrl::File(resolve_path(&path)?),
            DatasourceUrl::Url(_) => source_url,
        };

        let store_access = GenericStoreAccess::new_from_location_and_opts(
            url.to_string().as_str(),
            storage_options,
        )?;

        Ok(bson_streaming_table(Arc::new(store_access), Some(sample_size), url).await?)
    }
}
