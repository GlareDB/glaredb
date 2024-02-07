use std::any::Any;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion_ext::stream::StreamExecPlan;
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use protogen::metastore::types::options::StorageOptions;

use crate::common::util::create_count_record_batch;

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
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut stream = execute_stream(input, state.task_ctx())?.chunks(32);
        let mut ds = self.dataset.clone();
        let schema = self.schema().clone();

        Ok(Arc::new(StreamExecPlan::new(
            schema.clone(),
            Mutex::new(Some(Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::once(async move {
                    let write_opts = WriteParams {
                        mode: WriteMode::Append,
                        ..Default::default()
                    };

                    let mut count: u64 = 0;
                    while let Some(batches) = stream.next().await {
                        let start = ds.count_rows().await?;
                        let rbi = RecordBatchIterator::new(
                            batches
                                .into_iter()
                                .map(|v| v.map_err(|dfe| ArrowError::ExternalError(Box::new(dfe)))),
                            schema.clone(),
                        );
                        ds.append(rbi, Some(write_opts.clone())).await?;
                        count += (ds.count_rows().await? - start) as u64;
                    }
                    Ok::<RecordBatch, DataFusionError>(create_count_record_batch(count))
                }),
            )))),
        )))
    }
}
