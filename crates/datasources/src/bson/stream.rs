use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::Stream;
use futures::StreamExt;

use bson::Document;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;

use super::builder::RecordStructBuilder;

pub struct BsonStream {
    schema: Arc<Schema>,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
}

impl Stream for BsonStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for BsonStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl BsonStream {
    pub fn new(
        schema: Arc<Schema>,
        docs: Pin<Box<dyn Stream<Item = Result<Document, DataFusionError>> + Send>>,
    ) -> Self {
        let stream_schema = schema.clone();

        let stream = docs
            .chunks(100)
            .map(move |results| Self::convert_chunk(results, &stream_schema))
            .boxed();

        Self { schema, stream }
    }

    fn convert_chunk(
        results: Vec<Result<Document, DataFusionError>>,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut builder = RecordStructBuilder::new_with_capacity(schema.fields().to_owned(), 100)?;

        for result in results {
            let item = result?;
            let raw = bson::RawDocumentBuf::try_from(&item)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            builder.append_record(&raw)?;
        }

        let (fields, builders) = builder.into_fields_and_builders();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            builders.into_iter().map(|mut col| col.finish()).collect(),
        )?;

        Ok(batch)
    }
}

pub type SendableDocumentStream =
    Pin<Box<dyn Stream<Item = Result<Document, DataFusionError>> + Send>>;

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
