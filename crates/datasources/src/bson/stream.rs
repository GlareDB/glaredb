use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bson::RawDocumentBuf;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use futures::StreamExt;

use super::builder::RecordStructBuilder;
use super::errors::BsonError;

pub type SendableDocumentStream =
    Pin<Box<dyn Stream<Item = Result<RawDocumentBuf, BsonError>> + Send>>;

pub struct BsonStream {
    schema: Arc<Schema>,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, BsonError>> + Send>>,
}

impl Stream for BsonStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream
            .poll_next_unpin(cx)
            .map_err(DataFusionError::from)
    }
}

impl RecordBatchStream for BsonStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl BsonStream {
    pub fn new(schema: Arc<Schema>, docs: SendableDocumentStream) -> Self {
        let stream_schema = schema.clone();

        let stream = docs
            .chunks(100)
            .map(move |results| Self::convert_chunk(results, &stream_schema))
            .boxed();

        Self { schema, stream }
    }

    fn convert_chunk(
        results: Vec<Result<RawDocumentBuf, BsonError>>,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, BsonError> {
        let mut builder = RecordStructBuilder::new_with_capacity(schema.fields().to_owned(), 100)?;

        for result in results {
            // TOOD: shouldn't convert here.
            builder.append_record(&result?)?;
        }

        let (fields, builders) = builder.into_fields_and_builders();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            builders.into_iter().map(|mut col| col.finish()).collect(),
        )?;

        Ok(batch)
    }
}

pub struct BsonPartitionStream {
    schema: Arc<Schema>,
    stream: Mutex<Option<SendableDocumentStream>>,
}

impl PartitionStream for BsonPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let partition = self
            .stream
            .lock()
            .unwrap()
            .take()
            .expect("stream to only be called once")
            .boxed();
        Box::pin(BsonStream::new(self.schema.clone(), partition))
    }
}

impl BsonPartitionStream {
    pub fn new(schema: Arc<Schema>, stream: SendableDocumentStream) -> Self {
        Self {
            schema,
            stream: Mutex::new(Some(stream)),
        }
    }
}
