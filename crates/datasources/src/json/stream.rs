use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use futures::StreamExt;
use serde_json::{Map, Value};

pub struct JsonStream {
    schema: Arc<Schema>,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
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
    stream: Mutex<Option<Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>>>,
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
        let stream = futures::stream::iter(chunk.into_iter())
            .chunks(25)
            .map(move |objs| {
                let mut decoder =
                    ReaderBuilder::new(stream_schema.clone().to_owned()).build_decoder()?;
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
