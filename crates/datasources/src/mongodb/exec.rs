use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_stream::stream;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    RecordBatchStream,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use futures::{Stream, StreamExt};
use mongodb::bson::RawDocumentBuf;
use mongodb::Cursor;

use super::errors::{MongoDbError, Result};
use crate::bson::builder::RecordStructBuilder;

#[derive(Debug)]
pub struct MongoDbBsonExec {
    cursor: Mutex<Option<Cursor<RawDocumentBuf>>>,
    schema: Arc<ArrowSchema>,
    limit: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
}

impl MongoDbBsonExec {
    pub fn new(
        cursor: Mutex<Option<Cursor<RawDocumentBuf>>>,
        schema: Arc<ArrowSchema>,
        limit: Option<usize>,
    ) -> MongoDbBsonExec {
        MongoDbBsonExec {
            cursor,
            schema,
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for MongoDbBsonExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for MongoDB Exec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "only single partition supported".to_string(),
            ));
        }

        let cursor = {
            let cursor = self.cursor.lock();
            cursor.unwrap().take().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "execution called on partition {partition} more than once"
                ))
            })?
        };

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            BsonStream::new(cursor, self.schema.clone(), self.limit),
            partition,
            &self.metrics,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for MongoDbBsonExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MongoBsonExec")
    }
}

struct BsonStream {
    schema: Arc<ArrowSchema>,
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}

impl BsonStream {
    fn new(cursor: Cursor<RawDocumentBuf>, schema: Arc<ArrowSchema>, limit: Option<usize>) -> Self {
        let schema_stream = schema.clone();
        let mut row_count = 0;
        // Build "inner" stream.
        let stream = stream! {
            let mut chunked = cursor.chunks(100);
            while let Some(result) = chunked.next().await {
                let result = document_chunk_to_record_batch(result, schema_stream.fields.clone());
                match result {
                    Ok(batch) => {
                        let len = batch.num_rows();
                        yield Ok(batch);
                        row_count += len;
                        if let Some(limit) = limit {
                            if row_count > limit {
                                return
                            }
                        }
                    },
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(e)));
                        return;
                    }
                }
            }
        };

        BsonStream {
            schema,
            inner: Box::pin(stream),
        }
    }
}

impl Stream for BsonStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for BsonStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

fn document_chunk_to_record_batch<E: Into<MongoDbError>>(
    chunk: Vec<Result<RawDocumentBuf, E>>,
    fields: Fields,
) -> Result<RecordBatch> {
    let chunk = chunk
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.into())?;

    let mut builder = RecordStructBuilder::new_with_capacity(fields, chunk.len())?;
    for doc in chunk {
        builder.append_record(&doc)?;
    }

    let (fields, builders) = builder.into_fields_and_builders();
    let cols: Vec<Arc<dyn Array>> = builders.into_iter().map(|mut col| col.finish()).collect();
    let schema = ArrowSchema::new(fields);

    let batch = RecordBatch::try_new(Arc::new(schema), cols)?;
    Ok(batch)
}
