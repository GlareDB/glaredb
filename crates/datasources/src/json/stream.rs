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
use jaq_interpret::{Ctx, Filter, FilterT, RcIter, Val};
use json_stream::JsonStream;
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Map, Value};

use crate::json::errors::{JsonError, Result};

pub type CheckedRecordBatchStream = Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>;

/// VectorPartition wraps a vector of serde_json documents as a
/// Partition as one of DataFusion's streaming table. Well all of
pub struct VectorPartition {
    schema: Arc<Schema>,
    objs: Vec<Map<String, Value>>,
}

impl PartitionStream for VectorPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(JsonHandler::new(
            self.schema.clone(),
            futures::stream::iter(self.objs.clone().into_iter().map(Ok)).boxed(),
        ))
    }
}

impl VectorPartition {
    pub fn new(schema: Arc<Schema>, objs: Vec<Map<String, Value>>) -> Self {
        Self { schema, objs }
    }
}

/// ObjectStorePartition holds a reference to the object store and the
/// object metadata and represents a partition that is read only when
/// the partition is executed.
pub(crate) struct ObjectStorePartition {
    schema: Arc<Schema>,
    store: Arc<dyn ObjectStore>,
    obj: ObjectMeta,
    filter: Option<Arc<Filter>>,
}

impl ObjectStorePartition {
    pub fn new(
        schema: Arc<Schema>,
        store: Arc<dyn ObjectStore>,
        obj: ObjectMeta,
        filter: Option<Arc<Filter>>,
    ) -> Self {
        Self {
            schema,
            store,
            obj,
            filter,
        }
    }
}

impl PartitionStream for ObjectStorePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(JsonHandler::new_from_object_store(
            self.schema.to_owned(),
            self.store.clone(),
            self.obj.clone(),
            self.filter.clone(),
        ))
    }
}

/// JsonObjectStream represents a sequence of "json documents" in an
/// intermediate format produced by serde_json.
type JsonObjectStream = Pin<Box<dyn Stream<Item = Result<Map<String, Value>>> + Send>>;

/// JsonHandler is the basis of all stream handling, converting
/// streams of serde_json objects to RecordBatches, including from
/// object store and from iterators of values.
///
/// These are used by the PartitionStream implementations (above)
/// which interface into the table function and providers.
struct JsonHandler {
    schema: Arc<Schema>,
    stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl RecordBatchStream for JsonHandler {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for JsonHandler {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl JsonHandler {
    fn new(schema: Arc<Schema>, stream: JsonObjectStream) -> Self {
        let stream = Self::convert_stream(schema.clone(), stream);
        Self { schema, stream }
    }

    fn new_from_object_store(
        schema: Arc<Schema>,
        store: Arc<dyn ObjectStore>,
        obj: ObjectMeta,
        jaq_filter: Option<Arc<Filter>>,
    ) -> Self {
        let stream_schema = schema.clone();
        let jaq_filter = jaq_filter.clone();

        let stream = futures::stream::once(async move {
            let store = store.clone();
            let filter = jaq_filter.clone();

            Self::convert_stream(
                stream_schema,
                JsonStream::<Value, _>::new(match store.get(&obj.location).await {
                    Ok(stream) => stream.into_stream().map_err(JsonError::from),
                    Err(e) => return futures::stream::once(async move { Err(e.into()) }).boxed(),
                })
                .flat_map(move |v| Self::unwind_json_value(v, &filter))
                .boxed(),
            )
        })
        .flatten()
        .boxed();

        Self { schema, stream }
    }

    fn convert_stream(schema: Arc<Schema>, input: JsonObjectStream) -> CheckedRecordBatchStream {
        input
            .map(|rb| rb.map_err(JsonError::from))
            .chunks(1024)
            .map(move |chunk| {
                let chunk = chunk.into_iter().collect::<Result<Vec<_>>>()?;
                let mut decoder = ReaderBuilder::new(schema.to_owned()).build_decoder()?;
                decoder
                    .serialize(&chunk)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(decoder.flush()?.unwrap())
            })
            .boxed()
    }

    fn unwind_json_value(input: Result<Value>, filter: &Option<Arc<Filter>>) -> JsonObjectStream {
        futures::stream::iter(match input {
            Ok(value) => {
                let res = match filter {
                    Some(jq) => {
                        let inputs = RcIter::new(core::iter::empty());
                        match jq
                            .run((Ctx::new([], &inputs), Val::from(value)))
                            .map(|res| res.map(Value::from))
                            .collect::<Result<Vec<_>, _>>()
                        {
                            Ok(vals) => Ok(Value::from_iter(vals)),
                            Err(e) => Err(JsonError::from(e)),
                        }
                    }
                    None => Ok(value),
                };


                match res {
                    Ok(value) => match value {
                        Value::Array(vals) => {
                            let mut out = Vec::with_capacity(vals.len());
                            for v in vals {
                                match v {
                                    Value::Object(doc) => out.push(Ok(doc)),
                                    Value::Null => out.push(Ok(Map::new())),
                                    Value::Array(_) => {
                                        out.push(if filter.is_some() {
                                            Err(JsonError::UnsupportedNestedArray(
                                                "must return objects or nulls from jaq expressions",
                                            ))
                                        } else {
                                            Err(JsonError::UnsupportedType(
                                                "arrays must contain objects or nulls",
                                            ))
                                        });
                                        break;
                                    }
                                    _ => {
                                        out.push(Err(JsonError::UnsupportedNestedArray(
                                            "scalars are not supported",
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
                            vec![Err(JsonError::UnsupportedType(
                                "only objects and arrays of objects are supported",
                            ))]
                        }
                    },
                    Err(e) => vec![Err(e)],
                }
            }
            Err(e) => vec![Err(e)],
        })
        .boxed()
    }
}
