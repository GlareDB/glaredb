use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Map, Value};

use crate::json::errors::JsonError;
use crate::json::table::push_unwind_json_values;

pub type SendableCheckedRecordBatchStrem =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>;

pub struct JsonStream {
    schema: Arc<Schema>,
    stream: SendableCheckedRecordBatchStrem,
}

impl Stream for JsonStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for JsonStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct JsonPartitionStream {
    schema: Arc<Schema>,
    stream: Mutex<Option<SendableCheckedRecordBatchStrem>>,
}

impl PartitionStream for JsonPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let partition = self
            .stream
            .lock()
            .unwrap()
            .take()
            .expect("stream can only be used once")
            .boxed();

        Box::pin(JsonStream {
            schema: self.schema.clone(),
            stream: partition,
        })
    }
}

impl JsonPartitionStream {
    pub fn new(schema: Arc<Schema>, chunk: Vec<Map<String, Value>>) -> Self {
        let stream_schema = schema.clone();
        let stream = futures::stream::iter(chunk)
            .chunks(1000)
            .map(move |objs| {
                let mut decoder = ReaderBuilder::new(stream_schema.to_owned()).build_decoder()?;
                decoder
                    .serialize(&objs)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(decoder.flush()?.unwrap())
            })
            .boxed();

        Self {
            schema: schema.clone(),
            stream: Mutex::new(Some(stream)),
        }
    }
}

pub(crate) struct LazyJsonPartitionStream {
    schema: Arc<Schema>,
    store: Arc<dyn ObjectStore>,
    obj: ObjectMeta,
}

impl PartitionStream for LazyJsonPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream_schema = self.schema.to_owned();
        let store = self.store.clone();
        let obj = self.obj.clone();

        Box::pin(JsonStream {
            schema: self.schema.clone(),
            stream: futures::stream::once(async move {
                futures::stream::iter(match Self::build(stream_schema, store, obj).await {
                    Ok(batches) => batches,
                    Err(e) => vec![Err(DataFusionError::External(Box::new(e)))],
                })
            })
            .flatten()
            .boxed(),
        })
    }
}

impl LazyJsonPartitionStream {
    pub fn new(schema: Arc<Schema>, store: Arc<dyn ObjectStore>, obj: ObjectMeta) -> Self {
        Self { schema, store, obj }
    }

    async fn build(
        schema: Arc<Schema>,
        store: Arc<dyn ObjectStore>,
        obj: ObjectMeta,
    ) -> Result<Vec<Result<RecordBatch, DataFusionError>>, JsonError> {
        let mut data = Vec::new();
        push_unwind_json_values(
            &mut data,
            serde_json::from_slice::<Value>(&store.get(&obj.location).await?.bytes().await?),
        )?;

        Ok(data
            .chunks(1000)
            .map(|chunk| {
                let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
                decoder
                    .serialize(chunk)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(decoder.flush()?.unwrap())
            })
            .collect())
    }
}
