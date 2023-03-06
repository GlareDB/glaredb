use crate::errors::{MongoError, Result};
use async_stream::stream;
use bitvec::{order::Lsb0, vec::BitVec};
use datafusion::arrow::array::{
    Array, ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
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
use mongodb::bson::{doc, Document, RawDocumentBuf};
use mongodb::{options::ClientOptions, Client, Collection};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone)]
pub struct MongoBsonExec {
    schema: Arc<ArrowSchema>,
    limit: Option<usize>,
    collection: Collection<RawDocumentBuf>,
}

impl ExecutionPlan for MongoBsonExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(0)
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
        unimplemented!()
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

        // Build schema index (field name -> column index)
        let mut schema_index = HashMap::with_capacity(schema.fields.len());
        for (idx, field) in schema.fields.iter().enumerate() {
            schema_index.insert(field.name().clone(), idx);
        }

        let schema_stream = schema.clone();
        let stream = stream! {
            let cursor = match collection.find(None, None).await {
                Ok(cursor) => cursor,
                Err(e) => {
                    yield Err(DataFusionError::External(Box::new(e)));
                    return;
                }
            };

            let mut chunked = cursor.chunks(100);
            while let Some(result) = chunked.next().await {
                let batch = document_chunk_to_record_batch(result, schema_stream.clone(), &schema_index);
                // match result {
                //     Ok(doc) => {
                //         // do stuff
                //     },
                //     Err(e) => {
                //         yield Err(DataFusionError::External(Box::new(e)));
                //         return;
                //     }
                // }
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
        unimplemented!()
    }
}

impl RecordBatchStream for BsonStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

fn document_chunk_to_record_batch<E: Into<MongoError>>(
    chunk: Vec<Result<RawDocumentBuf, E>>,
    schema: Arc<ArrowSchema>,
    schema_index: &HashMap<String, usize>,
) -> Result<RecordBatch> {
    let chunk = chunk
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.into())?;

    let mut cols = column_builders_for_schema(&schema, chunk.len())?;

    for doc in chunk {
        let mut cols_set: BitVec<u8, Lsb0> = BitVec::repeat(false, schema.fields.len());

        for iter_result in doc.iter() {
            match iter_result {
                Ok((key, val)) => {
                    let idx = *schema_index
                        .get(key)
                        .ok_or_else(|| MongoError::ColumnNotInInferredSchema(key.to_string()))?;

                    // Add to cols...

                    cols_set.set(idx, true);
                }
                Err(_) => return Err(MongoError::FailedToReadRawBsonDocument),
            }
        }

        // Append nulls to all columns not included in the doc.
        for (idx, did_set) in cols_set.iter().enumerate() {
            if !did_set {
                // Add nulls...
            }
        }
    }

    let cols: Vec<Arc<dyn Array>> = cols.into_iter().map(|mut col| col.finish()).collect();

    let batch = RecordBatch::try_new(schema, cols)?;
    Ok(batch)
}

fn column_builders_for_schema(
    schema: &ArrowSchema,
    capacity: usize,
) -> Result<Vec<Box<dyn ArrayBuilder>>> {
    let mut cols = Vec::with_capacity(capacity);

    for field in &schema.fields {
        let col: Box<dyn ArrayBuilder> = match field.data_type() {
            &DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
            &DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
            &DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
            &DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
            &DataType::Timestamp(_, _) => {
                Box::new(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            &DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            &DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            other => return Err(MongoError::UnexpectedDataTypeForBuilder(other.clone())),
        };

        cols.push(col);
    }

    Ok(cols)
}
