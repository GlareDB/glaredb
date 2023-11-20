use async_trait::async_trait;
use datafusion::arrow::json::writer::{JsonArray, JsonFormat, LineDelimited, Writer as JsonWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::common::errors::Result;

use super::SharedBuffer;

const BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct JsonSinkOpts {
    /// If the batches should be written out as a json array.
    pub array: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for JsonSinkOpts {
    fn default() -> Self {
        JsonSinkOpts { array: false }
    }
}

#[derive(Debug)]
pub struct JsonSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
    opts: JsonSinkOpts,
}

impl Display for JsonSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonSink({}:{})", self.store, self.loc)
    }
}

impl DisplayAs for JsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{self}"),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl JsonSink {
    pub fn from_obj_store(
        store: Arc<dyn ObjectStore>,
        loc: impl Into<ObjectPath>,
        opts: JsonSinkOpts,
    ) -> JsonSink {
        JsonSink {
            store,
            loc: loc.into(),
            opts,
        }
    }

    async fn stream_into_inner(&self, stream: SendableRecordBatchStream) -> Result<usize> {
        Ok(if self.opts.array {
            self.formatted_stream::<JsonArray>(stream).await?
        } else {
            self.formatted_stream::<LineDelimited>(stream).await?
        })
    }

    async fn formatted_stream<F: JsonFormat>(
        &self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<usize> {
        let (_id, obj_handle) = self.store.put_multipart(&self.loc).await?;
        let mut writer = AsyncJsonWriter::<_, F>::new(obj_handle, BUFFER_SIZE);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write_batch(batch).await?;
        }
        writer.finish().await
    }
}

#[async_trait]
impl DataSink for JsonSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let count = self
            .stream_into_inner(data)
            .await
            .map(|x| x as u64)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(count)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
}

/// Wrapper around Arrow's json writer to provide async write support.
///
/// Modeled after the parquet crate's `AsyncArrowWriter`.
struct AsyncJsonWriter<W, F: JsonFormat> {
    async_writer: W,
    sync_writer: JsonWriter<SharedBuffer, F>,
    buffer: SharedBuffer,
    row_count: usize,
}

impl<W: AsyncWrite + Unpin + Send, F: JsonFormat> AsyncJsonWriter<W, F> {
    fn new(async_writer: W, buf_size: usize) -> Self {
        let buf = SharedBuffer::with_capacity(buf_size);
        let sync_writer = JsonWriter::new(buf.clone());
        AsyncJsonWriter {
            async_writer,
            sync_writer,
            buffer: buf,
            row_count: 0,
        }
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        self.sync_writer.write(&batch)?;
        self.try_flush(false).await?;
        self.row_count += num_rows;
        Ok(())
    }

    async fn finish(mut self) -> Result<usize> {
        self.sync_writer.finish()?;
        self.try_flush(true).await?;
        self.async_writer.shutdown().await?;
        Ok(self.row_count)
    }

    async fn try_flush(&mut self, force: bool) -> Result<()> {
        let mut buf = self.buffer.buffer.try_lock().unwrap();
        if !force && buf.len() < buf.capacity() / 2 {
            return Ok(());
        }

        self.async_writer.write_all(&buf).await?;
        self.async_writer.flush().await?;

        buf.clear();

        Ok(())
    }
}
