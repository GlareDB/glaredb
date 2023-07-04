use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::common::errors::Result;

use super::SharedBuffer;

const BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct CsvSinkOpts {
    /// Delimiter between values.
    pub delim: u8,
    /// Include header.
    pub header: bool,
}

impl Default for CsvSinkOpts {
    fn default() -> Self {
        CsvSinkOpts {
            delim: b',',
            header: true,
        }
    }
}

#[derive(Debug)]
pub struct CsvSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
    opts: CsvSinkOpts,
}

impl fmt::Display for CsvSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CsvSink({}:{})", self.store, self.loc)
    }
}

impl CsvSink {
    pub fn from_obj_store(
        store: Arc<dyn ObjectStore>,
        loc: impl Into<ObjectPath>,
        opts: CsvSinkOpts,
    ) -> CsvSink {
        CsvSink {
            store,
            loc: loc.into(),
            opts,
        }
    }

    async fn stream_into_inner(&self, mut stream: SendableRecordBatchStream) -> Result<usize> {
        let (_id, obj_handle) = self.store.put_multipart(&self.loc).await?;
        let mut writer = AsyncCsvWriter::new(obj_handle, BUFFER_SIZE, &self.opts);

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write_batch(&batch).await?;
        }
        writer.finish().await?;

        Ok(0)
    }
}

#[async_trait]
impl DataSink for CsvSink {
    async fn write_all(&self, stream: SendableRecordBatchStream) -> DfResult<u64> {
        self.stream_into_inner(stream)
            .await
            .map(|x| x as u64)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

/// Wrapper around Arrow's csv writer to provide async write support.
///
/// Modeled after the parquet crate's `AsyncArrowWriter`.
struct AsyncCsvWriter<W> {
    async_writer: W,
    sync_writer: CsvWriter<SharedBuffer>,
    buffer: SharedBuffer,
    row_count: usize,
}

impl<W: AsyncWrite + Unpin + Send> AsyncCsvWriter<W> {
    fn new(async_writer: W, buf_size: usize, sink_opts: &CsvSinkOpts) -> Self {
        let buf = SharedBuffer::with_capacity(buf_size);
        let sync_writer = CsvWriterBuilder::new()
            .with_delimiter(sink_opts.delim)
            .has_headers(sink_opts.header)
            .build(buf.clone());

        AsyncCsvWriter {
            async_writer,
            sync_writer,
            buffer: buf,
            row_count: 0,
        }
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.sync_writer.write(batch)?;
        self.try_flush(false).await?;
        self.row_count += batch.num_rows();
        Ok(())
    }

    async fn finish(mut self) -> Result<usize> {
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
