use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
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
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use futures::StreamExt;
use mongodb::bson::RawDocumentBuf;
use mongodb::Collection;

use crate::common::util::{create_count_record_batch, COUNT_SCHEMA};

pub struct MongoDbInsertExecPlan {
    collection: Collection<RawDocumentBuf>,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl MongoDbInsertExecPlan {
    pub fn new(collection: Collection<RawDocumentBuf>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            collection,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for MongoDbInsertExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MongoDbInsertExecPlan")
    }
}

impl std::fmt::Debug for MongoDbInsertExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MongoDbInsertExecPlan: {:?}", self.schema())
    }
}

impl ExecutionPlan for MongoDbInsertExecPlan {
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
                "cannot replace children for MongoDbInsertExecPlan".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "cannot partition mongodb insert exec".to_string(),
            ));
        }

        let mut input = execute_stream(self.input.clone(), ctx)?;
        let coll = self.collection.clone();

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            RecordBatchStreamAdapter::new(
                COUNT_SCHEMA.clone(),
                futures::stream::once(async move {
                    let mut count: u64 = 0;
                    while let Some(batch) = input.next().await {
                        let rb = batch?;

                        let mut docs = Vec::with_capacity(rb.num_rows());
                        let converted = crate::bson::BsonBatchConverter::from_record_batch(rb);

                        for d in converted.into_iter() {
                            let doc = d.map_err(|e| DataFusionError::Execution(e.to_string()))?;

                            docs.push(
                                RawDocumentBuf::from_document(&doc)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?,
                            );
                        }

                        count += coll
                            .insert_many(docs, None)
                            .await
                            .map(|res| res.inserted_ids.len())
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            as u64;
                    }
                    Ok::<RecordBatch, DataFusionError>(create_count_record_batch(count))
                })
                .boxed(),
            ),
            partition,
            &self.metrics,
        )))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
