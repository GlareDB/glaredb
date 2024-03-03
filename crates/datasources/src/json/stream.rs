use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Map, Value};

use super::multi::JsonStreamHandler;

pub type SendableCheckedRecordBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>;

pub struct JsonRecordBatchStream {
    schema: Arc<Schema>,
    stream: SendableCheckedRecordBatchStream,
}

impl Stream for JsonRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for JsonRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct JsonPartitionStream {
    schema: Arc<Schema>,
    stream: Vec<Map<String, Value>>,
}

impl PartitionStream for JsonPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(JsonRecordBatchStream {
            schema: self.schema.clone(),
            stream: JsonStreamHandler::new(
                self.schema.clone(),
                futures::stream::iter(self.stream.clone().into_iter().map(Ok)).boxed(),
            )
            .boxed(),
        })
    }
}

impl JsonPartitionStream {
    pub fn new(schema: Arc<Schema>, chunk: Vec<Map<String, Value>>) -> Self {
        Self {
            schema: schema.clone(),
            stream: chunk,
        }
    }
}

pub(crate) struct LazyJsonPartitionStream {
    schema: Arc<Schema>,
    store: Arc<dyn ObjectStore>,
    obj: ObjectMeta,
}

impl LazyJsonPartitionStream {
    pub fn new(schema: Arc<Schema>, store: Arc<dyn ObjectStore>, obj: ObjectMeta) -> Self {
        Self { schema, store, obj }
    }
}

impl PartitionStream for LazyJsonPartitionStream {
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
                    Err(e) => JsonStreamHandler::new_error(e),
                }
            })
            .flatten()
            .boxed(),
        })
    }
}
