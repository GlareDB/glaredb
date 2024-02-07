use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use lance::dataset::builder::DatasetBuilder;
use lance::Dataset;
use protogen::metastore::types::options::StorageOptions;

pub struct LanceTable {
    dataset: Dataset,
}

impl LanceTable {
    pub async fn new(location: &str, options: StorageOptions) -> Result<Self> {
        Ok(LanceTable {
            dataset: DatasetBuilder::from_uri(location)
                .with_storage_options(options.inner.into_iter().collect())
                .load()
                .await?,
        })
    }

    fn as_table_provider(&self) -> impl TableProvider {
        self.dataset.clone()
    }
}

#[async_trait]
impl TableProvider for LanceTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.as_table_provider().schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filter: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self
            .as_table_provider()
            .scan(state, projection, filter, limit)
            .await?)
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "lance inserts not implemented".to_string(),
        ))
    }
}
