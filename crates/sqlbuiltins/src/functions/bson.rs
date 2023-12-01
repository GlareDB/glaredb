use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};

use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::object_store::generic::GenericStoreAccess;
use datasources::object_store::ObjStoreAccess;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::functions::table_location_and_opts;

#[derive(Debug, Clone, Copy)]
pub struct BsonScan {}

#[async_trait]
impl TableFunc for BsonScan {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Unspecified
    }

    fn name(&self) -> &str {
        "read_bson"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>, ExtensionError> {
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let store_access = GenericStoreAccess::new_from_location_and_opts(
            source_url.to_string().as_str(),
            storage_options,
        )
        .map_err(|e| ExtensionError::Arrow(e.into()))?;

        let store = store_access
            .create_store()
            .map_err(|e| ExtensionError::Arrow(e.into()))?
            .clone();
        let path = source_url.path();

        let list = store_access
            .list_globbed(&store, path.as_ref())
            .await
            .map_err(|e| ExtensionError::Arrow(e.into()))?;

        if list.is_empty() {
            return Err(ExtensionError::String(format!(
                "no matching objects for '{path}'"
            )));
        }

        let _sample_size = match opts.get("schema_sample_size") {
            Some(v) => v.to_owned().param_into()?,
            None => 100,
        };

        // TODO: sample until we get to the number of sample size;

        // TODO: hold on to the sample; if we run out of documents we
        // should just return what we have; [big initial samples used
        // to just force everything into memory at once].

        // TODO: have an argument to read things serially. (and maybe
        // sort objects in the glob by name in this case, and an
        // option to do it anyway?)

        // TODO: take the inferred schema, and write a Partition
        // handler for every object, add then all to a vector and
        // return a StreamingTable.

        Ok(Arc::new(StreamingTable::try_new(
            Arc::new(Schema::empty()), // <= inferred schema
            Vec::new(),                // <= vector of partition scanners
        )?))
    }
}
