use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use json_stream::JsonStream;
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Map, Value};

use crate::json::errors::JsonError;

pub struct JsonStreamHandler {
    schema: Arc<Schema>,
    buf: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
    worker: tokio::task::JoinHandle<Result<(), DataFusionError>>,
}

impl Stream for JsonStreamHandler {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.buf.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                if self.worker.is_finished() {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Some(val)) => Poll::Ready(Some(val)),
        }
    }
}

impl RecordBatchStream for JsonStreamHandler {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl JsonStreamHandler {
    pub fn new(
        schema: Arc<Schema>,
        stream: Pin<Box<dyn Stream<Item = Result<Map<String, Value>, JsonError>> + Send>>,
    ) -> Self {
        let stream_schema = schema.clone();
        let (mut sink, buf) = mpsc::channel(2048);

        Self {
            schema,
            buf: buf
                .chunks(1024)
                .map(move |chunk| {
                    let mut decoder =
                        ReaderBuilder::new(stream_schema.to_owned()).build_decoder()?;
                    decoder
                        .serialize(&chunk)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Ok(decoder.flush()?.unwrap())
                })
                .boxed(),
            worker: tokio::spawn(async move {
                let mut stream = stream;
                while let Some(row) = stream.next().await {
                    sink.send(row?).await.map_err(JsonError::from)?;
                }
                Ok(())
            }),
        }
    }

    pub fn new_error(err: JsonError) -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            buf: futures::stream::empty().boxed(),
            worker: tokio::task::spawn_local(async move { Err(err.into()) }),
        }
    }

    pub async fn setup_read_stream(
        store: Arc<dyn ObjectStore>,
        obj: ObjectMeta,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Map<String, Value>, JsonError>> + Send>>, JsonError>
    {
        let vals = store
            .get(&obj.location)
            .await?
            .into_stream()
            .map_err(|e| JsonError::ObjectStore(e));

        Ok(JsonStream::<Value, _>::new(vals)
            .flat_map(Self::unwind_json_value)
            .boxed())
    }


    fn unwind_json_value(
        input: Result<Value, JsonError>,
    ) -> Pin<Box<dyn Stream<Item = Result<Map<String, Value>, JsonError>> + Send>> {
        futures::stream::iter(
            match input {
                Ok(value) => match value {
                    Value::Array(vals) => {
                        let mut out = Vec::with_capacity(vals.len());
                        for v in vals {
                            match v {
                                Value::Object(doc) => out.push(Ok(doc)),
                                Value::Null => out.push(Ok(Map::new())),
                                _ => out.push(Err(JsonError::UnspportedType(
                                    "only objects and arrays of objects are supported",
                                ))),
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
            }
            .into_iter(),
        )
        .boxed()
    }
}
