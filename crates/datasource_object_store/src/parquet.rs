//! Helpers for handling parquet files.
use std::any::Any;
use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use datafusion::arrow::datatypes::{SchemaRef as ArrowSchemaRef, SchemaRef};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use futures::{ready, Stream};
use object_store::{ObjectMeta, ObjectStore};

use crate::errors::Result;

pub type ParquetStreamOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, ArrowResult<RecordBatch>>>>;

/// Implement parquet's `AsyncFileReader` interface.
#[derive(Debug)]
pub struct ParquetObjectReader {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub meta_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| ParquetError::General(format!("get bytes: {e}")))
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
                .map_err(|e| ParquetError::General(format!("get ranges: {e}")))
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata =
                fetch_parquet_metadata(self.store.as_ref(), &self.meta, self.meta_size_hint)
                    .await
                    .map_err(|e| ParquetError::General(format!("fetch metadata: {e}")))?;
            Ok(Arc::new(metadata))
        })
    }
}

/// Open and stream the parquet file from object storage.
pub struct ParquetOpener {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub meta_size_hint: Option<usize>,
    pub projection: Option<Vec<usize>>,
}

impl ParquetOpener {
    pub fn open(&self) -> Result<ParquetStreamOpenFuture> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: self.meta_size_hint,
        };

        let projection = self.projection.clone();

        Ok(Box::pin(async move {
            let read_opts = ArrowReaderOptions::new().with_page_index(true);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, read_opts).await?;

            if let Some(projection) = projection {
                // Parquet schemas are tree like in that each field may contain
                // nested fields (e.g. a struct).
                //
                // "roots" here indicates we're masking on the top-level fields.
                let mask = ProjectionMask::roots(builder.parquet_schema(), projection);
                builder = builder.with_projection(mask);
            }

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

/// Retrive data from object storage with a parquet stream reader
#[derive(Debug)]
pub(crate) struct ParquetExec {
    pub(crate) arrow_schema: ArrowSchemaRef,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) meta: Arc<ObjectMeta>,
    pub(crate) projection: Option<Vec<usize>>,
}

impl ExecutionPlan for ParquetExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for ParquetExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let opener = ParquetOpener {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
            projection: self.projection.clone(),
        };

        let stream = ParquetFileStream {
            schema: self.arrow_schema.clone(),
            opener,
            state: StreamState::Idle,
        };

        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ParquetExec: location={}", self.meta.location)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
