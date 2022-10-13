//! Helpers for handling parquet files.
use crate::errors::Result;
use crate::partitionexec::PartitionMeta;
use crate::partitionexec::{PartitionOpenFuture, PartitionStreamOpener};
use bytes::Bytes;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{BoxStream, Stream, TryStream, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub struct ParquetObjectReader {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    meta_size_hint: Option<usize>,
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

pub struct ParquetOpener {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    meta_size_hint: Option<usize>,
}

impl ParquetOpener {}

impl PartitionStreamOpener for ParquetOpener {
    fn open(&self, meta: &PartitionMeta) -> Result<PartitionOpenFuture> {
        // TODO: Reduce cloning.
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: self.meta_size_hint.clone(),
        };

        Ok(Box::pin(async move {
            let read_opts = ArrowReaderOptions::new().with_page_index(true);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, read_opts).await?;

            // TODO: Add builder options.

            let reader = builder.build()?;
            let reader = reader.map_err(|e| e.into());

            let stream = Box::pin(reader) as BoxStream<'_, _>;
            Ok(stream)
        }))
    }
}
