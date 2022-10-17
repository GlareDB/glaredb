//! Helpers for handling parquet files.
use crate::errors::Result;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::properties::WriterProperties;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{BoxStream, TryStreamExt};
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{error, trace};

pub type ParquetStreamOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, ArrowResult<RecordBatch>>>>;

/// Implement parquet's `AsyncFileReader` interface.
#[derive(Debug)]
struct ParquetObjectReader {
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

/// Open and stream the parquet file from object storage.
pub struct ParquetOpener {
    pub store: Arc<dyn ObjectStore>,
    pub meta: ObjectMeta,
    pub meta_size_hint: Option<usize>,
    pub projection: Option<Vec<usize>>,
}

impl ParquetOpener {
    pub fn open(&self) -> Result<ParquetStreamOpenFuture> {
        // TODO: Reduce cloning.

        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: self.meta_size_hint.clone(),
        };

        let projection = self.projection.clone();

        Ok(Box::pin(async move {
            let read_opts = ArrowReaderOptions::new().with_page_index(true);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, read_opts).await?;

            // TODO: Add more builder options.

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

/// Upload record batches as a parquet file to object storage.
///
/// NOTE: This hopefully will be made redundant if upstream parquet gets an
/// async writer: https://github.com/apache/arrow-rs/issues/1269
pub struct ParquetUploader {
    pub store: Arc<dyn ObjectStore>,
    pub path: ObjectPath,
}

impl ParquetUploader {
    /// Upload in-memory batches to object storage.
    ///
    /// NOTE: The provided buffer will be used to hold the entire encoded
    /// parquet file in memory.
    pub async fn upload_batches(
        &self,
        schema: SchemaRef,
        batches: impl IntoIterator<Item = RecordBatch>,
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        // TODO: More options.
        let write_opts = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(buffer, schema, Some(write_opts))?;

        let batches = batches.into_iter();
        for batch in batches {
            writer.write(&batch)?;
        }

        // Flush and get the buffer reference back.
        let buffer = writer.into_inner()?;

        let (id, mut obj_writer) = self.store.put_multipart(&self.path).await?;
        if let Err(e) = obj_writer.write_all(buffer).await {
            trace!(%id, %e, "parquet upload failed, aborting multipart");
            if let Err(e) = self.store.abort_multipart(&self.path, &id).await {
                error!(%id, %e, "failed to abort multipart for parquet upload");
                // Don't return this error, return original error.
            }
            return Err(e.into());
        };

        if let Err(e) = obj_writer.shutdown().await {
            trace!(%id, %e, "object writer shutdown failed, aborting multipart");
            if let Err(e) = self.store.abort_multipart(&self.path, &id).await {
                error!(%id, %e, "failed to abort multipart after writer shutdown failure");
                // Same as above, return original error.
            }
            return Err(e.into());
        }

        Ok(())
    }
}
