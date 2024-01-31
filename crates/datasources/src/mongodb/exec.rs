use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_stream::stream;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::Precision;
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
    estimated_rows: u64,
}

impl MongoDbBsonExec {
    pub fn new(
        cursor: Mutex<Option<Cursor<RawDocumentBuf>>>,
        schema: Arc<ArrowSchema>,
        limit: Option<usize>,
        estimated_rows: u64,
    ) -> MongoDbBsonExec {
        MongoDbBsonExec {
            cursor,
            schema,
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
            estimated_rows,
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

    fn statistics(&self) -> DatafusionResult<Statistics> {
        let statistics = Statistics {
            num_rows: Precision::Inexact(self.estimated_rows as usize),
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.schema),
        };
        Ok(statistics)
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
                let result = document_chunk_to_record_batch(result, schema_stream.clone());
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
    schema: ArrowSchemaRef,
) -> Result<RecordBatch> {
    let chunk = chunk
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.into())?;

    let fields = schema.fields().clone();

    if fields.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(chunk.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options).map_err(|e| e.into());
    }

    let mut builder = RecordStructBuilder::new_with_capacity(fields, chunk.len())?;
    for doc in chunk {
        builder.append_record(&doc)?;
    }

    let mut builders = builder.into_builders();
    let cols: Vec<Arc<dyn Array>> = builders.iter_mut().map(|col| col.finish()).collect();

    let batch = RecordBatch::try_new(schema, cols)?;
    Ok(batch)
}
