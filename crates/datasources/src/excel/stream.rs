use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use calamine::{Data, Range};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};

use crate::excel;
use crate::excel::errors::ExcelError;

pub struct ExcelStream {
    schema: Arc<Schema>,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, ExcelError>> + Send>>,
}

impl ExcelStream {
    pub fn new(r: Range<Data>, header: bool, schema: Arc<Schema>) -> Self {
        let schema = schema.clone();
        let inferred_schema_length = r.width();

        let batch = excel::xlsx_sheet_value_to_record_batch(r, header, inferred_schema_length);
        let stream = Box::pin(futures::stream::iter(vec![batch]));

        Self { schema, stream }
    }
}

impl Stream for ExcelStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream
            .poll_next_unpin(cx)
            .map_err(DataFusionError::from)
    }
}

impl RecordBatchStream for ExcelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
