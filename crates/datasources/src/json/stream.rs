use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt, TryStreamExt};
use json_stream::JsonStream;
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Map, Value};

use crate::json::errors::{JsonError, Result};

pub struct JsonRecordBatchStream {
    schema: Arc<Schema>,
    // this is the same as a sendable recordbatch stream, but declared
    // separtley so we can have isomorphic values using adapters.
    stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl Stream for JsonRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for JsonRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// WrappedPartition wraps a vector of serde_json documents as a
/// Partition as one of DataFusion's streaming table. Well all of
pub struct WrappedPartition {
    schema: Arc<Schema>,
    stream: Vec<Map<String, Value>>,
}

impl PartitionStream for WrappedPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(JsonStreamHandler::new(
            self.schema.clone(),
            futures::stream::iter(self.stream.clone().into_iter().map(Ok)).boxed(),
        ))
    }
}

impl WrappedPartition {
    pub fn new(schema: Arc<Schema>, chunk: Vec<Map<String, Value>>) -> Self {
        Self {
            schema: schema.clone(),
            stream: chunk,
        }
    }
}

/// ObjectStorePartition holds a reference to the object store and the
/// object metadata and represents a partition that is read only when
/// the partition is executed.
pub(crate) struct ObjectStorePartition {
    schema: Arc<Schema>,
    store: Arc<dyn ObjectStore>,
    obj: ObjectMeta,
}

impl ObjectStorePartition {
    pub fn new(schema: Arc<Schema>, store: Arc<dyn ObjectStore>, obj: ObjectMeta) -> Self {
        Self { schema, store, obj }
    }
}

impl PartitionStream for ObjectStorePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream_schema = self.schema.to_owned();
        let store = self.store.clone();
        let obj = self.obj.clone();

        Box::pin(JsonRecordBatchStream {
            schema: self.schema.clone(),
            stream: futures::stream::once(async move {
                match JsonStreamHandler::setup_read_stream(store, obj).await {
                    Ok(st) => JsonStreamHandler::new(stream_schema, st),
                    Err(e) => JsonStreamHandler::wrap_error(e),
                }
            })
            .flatten()
            .boxed(),
        })
    }
}

/// JsonObjectStream represents a sequence of "json documents" in an
/// intermediate format produced by serde_json.
type JsonObjectStream = Pin<Box<dyn Stream<Item = Result<Map<String, Value>>> + Send>>;

/// JsonStreamHandler is the basis of all stream handling, converting
/// streams of serde_json objects to RecordBatches, including from
/// object store and from iterators of values.
///
/// These are used by the PartitionStream implementations (above)
/// which interface into the table function and providers.
struct JsonStreamHandler {
    schema: Arc<Schema>,
    buf: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl RecordBatchStream for JsonStreamHandler {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for JsonStreamHandler {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.buf.poll_next_unpin(cx)
    }
}

impl JsonStreamHandler {
    fn new(schema: Arc<Schema>, stream: JsonObjectStream) -> Self {
        let stream_schema = schema.clone();

        Self {
            schema,
            buf: stream
                .map(|rb| rb.map_err(JsonError::from))
                .chunks(1024)
                .map(move |chunk| {
                    let chunk = chunk.into_iter().collect::<Result<Vec<_>>>()?;
                    let mut decoder =
                        ReaderBuilder::new(stream_schema.to_owned()).build_decoder()?;
                    decoder
                        .serialize(&chunk)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Ok(decoder.flush()?.unwrap())
                })
                .boxed(),
        }
    }

    fn wrap_error(err: JsonError) -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            buf: futures::stream::once(async move { Err(err.into()) }).boxed(),
        }
    }

    async fn setup_read_stream(
        store: Arc<dyn ObjectStore>,
        obj: ObjectMeta,
    ) -> Result<JsonObjectStream> {
        Ok(JsonStream::<Value, _>::new(Box::pin(
            store
                .get(&obj.location)
                .await?
                .into_stream()
                .map_err(JsonError::from),
        ))
        .flat_map(Self::unwind_json_value)
        .boxed())
    }

    fn unwind_json_value(input: Result<Value>) -> JsonObjectStream {
        futures::stream::iter(match input {
            Ok(value) => match value {
                Value::Array(vals) => {
                    let mut out = Vec::with_capacity(vals.len());
                    for v in vals {
                        match v {
                            Value::Object(doc) => out.push(Ok(doc)),
                            Value::Null => out.push(Ok(Map::new())),
                            _ => {
                                out.push(Err(JsonError::UnspportedType(
                                    "only objects and arrays of objects are supported",
                                )));
                                break;
                            }
                        }
                    }
                    out
                }
                Value::Object(doc) => vec![Ok(doc)],
                Value::Null => vec![Ok(Map::new())],
                _ => {
                    vec![Err(JsonError::UnspportedType(
                        "only objects and arrays of objects are supported",
                    ))]
                }
            },
            Err(e) => vec![Err(e)],
        })
        .boxed()
    }
}
