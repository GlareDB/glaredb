//! Helpers for handling parquet files.
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::RecordBatchStream;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{BoxStream, TryStreamExt};
use futures::{ready, stream::StreamExt, Stream};
use object_store::{ObjectMeta, ObjectStore};

use crate::errors::Result;
pub type ParquetStreamOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, ArrowResult<RecordBatch>>>>;

/// Implement parquet's `AsyncFileReader` interface.
#[derive(Debug)]
pub struct ParquetObjectReader {
    pub store: Arc<dyn ObjectStore>,
    pub meta: ObjectMeta,
    pub meta_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| ParquetError::General(format!("get bytes: {}", e)))
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(async move {
            self.store
                .get_ranges(&self.meta.location, &ranges)
                .await
                .map_err(|e| ParquetError::General(format!("get ranges: {}", e)))
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata =
                fetch_parquet_metadata(self.store.as_ref(), &self.meta, self.meta_size_hint)
                    .await
                    .map_err(|e| ParquetError::General(format!("fetch metadata: {}", e)))?;
            Ok(Arc::new(metadata))
        })
    }
}

/// Open and stream the parquet file from object storage.
pub struct ParquetOpener {
    pub store: Arc<dyn ObjectStore>,
    pub meta: ObjectMeta,
    pub meta_size_hint: Option<usize>,
}

impl ParquetOpener {
    pub fn open(&self) -> Result<ParquetStreamOpenFuture> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: self.meta_size_hint,
        };

        Ok(Box::pin(async move {
            let read_opts = ArrowReaderOptions::new().with_page_index(true);
            let builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, read_opts).await?;

            let reader = builder.build()?.map_err(|e| e.into());

            let stream = Box::pin(reader) as BoxStream<'_, _>;
            Ok(stream)
        }))
    }
}

pub(crate) enum StreamState {
    /// File is not currently being read.
    Idle,
    /// File is being opened.
    Open { fut: ParquetStreamOpenFuture },
    /// The file completed opening with the given stream.
    Scan {
        stream: BoxStream<'static, ArrowResult<RecordBatch>>,
    },
    /// Done scanning.
    Done,
    /// Encountered an error.
    Error,
}

pub struct ParquetFileStream {
    pub(crate) schema: SchemaRef,
    pub(crate) opener: ParquetOpener,
    pub(crate) state: StreamState,
}

impl ParquetFileStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Idle => match self.opener.open() {
                    Ok(fut) => {
                        self.state = StreamState::Open { fut };
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                },
                StreamState::Open { fut } => match ready!(fut.poll_unpin(cx)) {
                    Ok(stream) => {
                        self.state = StreamState::Scan { stream };
                    }
                    Err(e) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                },
                StreamState::Scan { stream } => match ready!(stream.poll_next_unpin(cx)) {
                    Some(result) => {
                        return Poll::Ready(Some(result));
                    }
                    None => {
                        self.state = StreamState::Done;
                    }
                },
                StreamState::Done | StreamState::Error => return Poll::Ready(None),
            }
        }
    }
}

impl Stream for ParquetFileStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl RecordBatchStream for ParquetFileStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
