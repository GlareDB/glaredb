use async_stream::stream;
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bson::Document;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;

use super::builder::RecordStructBuilder;

pub struct BsonStream {
    schema: Arc<Schema>,
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
}

impl Stream for BsonStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for BsonStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl BsonStream {
    pub fn new(
        docs: Pin<Box<dyn Stream<Item = Result<Document, DataFusionError>>>>,
        schema: Arc<Schema>,
    ) -> Self {
        let bstream = stream! {
            let mut builder = RecordStructBuilder::new_with_capacity(schema.fields().to_owned(), 100)?;
            while let Some(item) = docs.next().await {
                match item {
                    Ok(doc) => {
                        let record: bson::RawDocument = doc.try_into()?.as_ref();
                        builder.append_record(&record)?;
                        if builder.len() >= 100 {
                            break
                        }
                    },
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                }
            }

            let (fields, builders) = builder.into_fields_and_builders();

            let out = RecordBatch::try_new(Arc::new(Schema::new(fields)),
                builders
                .into_iter()
                .map(|mut col| col.finish())
                .collect(),
            ).map_err(|e| DataFusionError::ArrowError(e))?;

            yield Ok(out);
        };

        BsonStream {
            schema: schema.clone(),
            inner: bstream,
        }
    }
}
