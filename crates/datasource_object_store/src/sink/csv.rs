use async_trait::async_trait;
use crate::errors::{ ObjectStoreSourceError, Result };
use datafusion::arrow::csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use datasource_common::sink::{Sink, SinkError};
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};

const BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct CsvSinkOpts {}

impl Default for CsvSinkOpts {
    fn default() -> Self {
        CsvSinkOpts {}
    }
}

pub struct CsvSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
    opts: CsvSinkOpts,
}

impl CsvSink {
    pub fn new(
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
        writer.close().await?;

        Ok(0)
    }
}

#[async_trait]
impl Sink for CsvSink {
    async fn stream_into(&self, stream: SendableRecordBatchStream) -> Result<usize, SinkError> {
        match self.stream_into_inner(stream).await {
            Ok(n) => Ok(n),
            Err(e) => Err(datasource_common::sink::SinkError::Boxed(Box::new(e))),
        }
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
        let sync_writer = CsvWriterBuilder::new().build(buf.clone());
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

    async fn close(mut self) -> Result<usize> {
        self.try_flush(true).await?;
        self.async_writer.shutdown().await?;
        Ok(self.row_count)
    }

    async fn try_flush(&mut self, force: bool) -> Result<()> {
        let mut buf = self.buffer.buffer.try_lock().unwrap();
        if !force && buf.len() < buf.capacity() / 2 {
            return Ok(());
        }

        self.async_writer.write(&buf).await?;
        self.async_writer.flush().await?;

        buf.clear();

        Ok(())
    }
}

#[derive(Clone)]
struct SharedBuffer {
    buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn with_capacity(cap: usize) -> Self {
        SharedBuffer {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(cap))),
        }
    }
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        std::io::Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        std::io::Write::flush(&mut *buffer)
    }
}
