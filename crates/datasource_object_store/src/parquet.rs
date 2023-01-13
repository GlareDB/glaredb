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
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::file_format::{
    FileMeta, FileScanConfig, ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::RecordBatchStream;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use futures::{ready, Stream};
use object_store::{ObjectMeta, ObjectStore};

use crate::errors::Result;

pub type ParquetStreamOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, ArrowResult<RecordBatch>>>>;

#[derive(Debug)]
pub struct SimpleParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
    base_config: FileScanConfig,
}

impl SimpleParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, base_config: FileScanConfig) -> Self {
        Self { store, base_config }
    }
}

impl ParquetFileReaderFactory for SimpleParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        //TODO: RUSTOM - REMOVE
        tracing::trace!("NEW READER");
        Ok(Box::new(ParquetObjectReader {
            store: self.store.clone(),
            meta: Arc::new(file_meta.object_meta),
            meta_size_hint: None,
        }))
    }
}

/// Implement parquet's `AsyncFileReader` interface.
#[derive(Debug)]
pub struct ParquetObjectReader {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub meta_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        //TODO: RUSTOM - REMOVE
        tracing::trace!("get_bytes");
        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| ParquetError::General(format!("get bytes: {}", e)))
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        //TODO: RUSTOM - REMOVE
        tracing::trace!("get_byte_ranges");
        Box::pin(async move {
            self.store
                .get_ranges(&self.meta.location, &ranges)
                .await
                .map_err(|e| ParquetError::General(format!("get ranges: {}", e)))
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        //TODO: RUSTOM - REMOVE
        tracing::trace!("get_metadata");
        Box::pin(async move {
            let metadata =
                fetch_parquet_metadata(self.store.as_ref(), &self.meta, self.meta_size_hint)
                    .await
                    .map_err(|e| ParquetError::General(format!("fetch metadata: {}", e)))?;
            Ok(Arc::new(metadata))
        })
    }
}
