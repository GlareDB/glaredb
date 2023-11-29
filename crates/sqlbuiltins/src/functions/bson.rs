use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::lake::storage_options_into_object_store;
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
        let (loc, opts) = table_location_and_opts(ctx, args, &mut opts)?;

        let _store = storage_options_into_object_store(&loc, &opts)
            .map_err(|err| ExtensionError::Access(Box::new(err)))?;

        Ok(Arc::new(BsonTableProvider {
            schema: Arc::new(Schema::empty()),
        }))
    }
}

pub struct BsonTableProvider {
    schema: Arc<Schema>,
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
        Err(DataFusionError::NotImplemented("bson scan".to_string()))
    }
}
