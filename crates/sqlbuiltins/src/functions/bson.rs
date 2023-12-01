use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
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

        let sample_size = match opts.get("schema_sample_size") {
            Some(v) => v.to_owned().param_into()?,
            None => 100,
        };

        Ok(Arc::new(BsonTableProvider {
            _sample_size: sample_size as usize,
            objects: list.to_owned(),
            store: Arc::new(store),
            schema: Arc::new(Schema::empty()),
        }))
    }
}

pub struct BsonTableProvider {
    objects: Vec<ObjectMeta>,
    store: Arc<dyn ObjectStore>,
    schema: Arc<Schema>,
    _sample_size: usize,
}

#[async_trait]
impl TableProvider for BsonTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if self.schema.fields.is_empty() {
            return Err(DataFusionError::Internal(
                "schema should have been inferred".to_string(),
            ));
        }

        for obj in &self.objects {
            let file = self.store.get(&obj.location);
            print!("{:?}", file.await?.type_id());
        }

        Err(DataFusionError::NotImplemented("bson scan".to_string()))
    }
}
