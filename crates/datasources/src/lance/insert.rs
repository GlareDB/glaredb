use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;

use crate::common::util::{create_count_record_batch, COUNT_SCHEMA};
pub(super) struct LanceInsertExecPlan {
    dataset: Dataset,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl LanceInsertExecPlan {
    pub(super) fn new(dataset: Dataset, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            dataset,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for LanceInsertExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LanceInsertExecPlan")
    }
}

impl std::fmt::Debug for LanceInsertExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LanceInsertExecPlan: {:?}", self.schema())
    }
}

impl ExecutionPlan for LanceInsertExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        COUNT_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for LanceInsertExecPlan".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut stream = execute_stream(self.input.clone(), ctx)?.chunks(32);
        let mut ds = self.dataset.clone();
        let schema = self.input.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
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
        )))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
