use crate::builder::RecordStructBuilder;
use crate::errors::{MongoError, Result};
use async_stream::stream;
use bitvec::{order::Lsb0, vec::BitVec};
use datafusion::arrow::array::{
    Array, ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    StructBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    display::DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use mongodb::bson::{doc, Document, RawBsonRef, RawDocumentBuf};
use mongodb::{
    options::{ClientOptions, FindOptions},
    Client, Collection,
};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Field name in mongo for uniquely identifying a record. Some special handling
/// needs to be done with the field when projecting.
const ID_FIELD_NAME: &str = "_id";

#[derive(Debug, Clone)]
pub struct MongoBsonExec {
    schema: Arc<ArrowSchema>,
    collection: Collection<RawDocumentBuf>,
    limit: Option<usize>,
}

impl MongoBsonExec {
    pub fn new(
        schema: Arc<ArrowSchema>,
        collection: Collection<RawDocumentBuf>,
        limit: Option<usize>,
    ) -> MongoBsonExec {
        MongoBsonExec {
            schema,
            collection,
            limit,
        }
    }
}

impl ExecutionPlan for MongoBsonExec {
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
            "cannot replace children for BigQueryExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(BsonStream::new(
            self.schema.clone(),
            self.collection.clone(),
            self.limit.clone(),
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MongoBsonExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct BsonStream {
    schema: Arc<ArrowSchema>,
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}

impl BsonStream {
    fn new(
        schema: Arc<ArrowSchema>,
        collection: Collection<RawDocumentBuf>,
        limit: Option<usize>,
    ) -> Self {
        // TODO: Filtering docs.

        // Projection document. Project everything that's in the schema.
        //
        // The `_id` field is special and needs to be manually suppressed if not
        // included in the schema.
        let mut proj_doc = Document::new();
        let mut has_id_field = false;
        for field in &schema.fields {
            proj_doc.insert(field.name(), 1);
            has_id_field = has_id_field || field.name().as_str() == ID_FIELD_NAME;
        }

        if !has_id_field {
            proj_doc.insert(ID_FIELD_NAME, 0);
        }

        let mut find_opts = FindOptions::default();
        find_opts.limit = limit.map(|v| v as i64);
        find_opts.projection = Some(proj_doc);

        let schema_stream = schema.clone();
        let mut row_count = 0;
        // Build "inner" stream.
        let stream = stream! {
            let cursor = match collection.find(None, Some(find_opts)).await {
                Ok(cursor) => cursor,
                Err(e) => {
                    yield Err(DataFusionError::External(Box::new(e)));
                    return;
                }
            };

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

fn document_chunk_to_record_batch<E: Into<MongoError>>(
    chunk: Vec<Result<RawDocumentBuf, E>>,
    fields: Vec<Field>,
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
