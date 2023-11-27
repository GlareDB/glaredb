use crate::bson;
use crate::common::errors::Result;
use async_trait::async_trait;
use datafusion::arrow::array::StructArray;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchWriter};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::{fmt::Debug, fmt::Display, io::Write, sync::Arc};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::SharedBuffer;

const BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub struct BsonSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
}

impl Display for BsonSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BsonSink({}:{})", self.store, self.loc)
    }
}

impl DisplayAs for BsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "BsonSink({}:{})", self.store, self.loc),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl BsonSink {
    pub fn from_obj_store(store: Arc<dyn ObjectStore>, loc: impl Into<ObjectPath>) -> BsonSink {
        BsonSink {
            store,
            loc: loc.into(),
        }
    }

    async fn pstream_into_inner(&self, stream: SendableRecordBatchStream) -> Result<usize> {
        self.formatted_stream(stream).await
    }

    async fn formatted_stream(&self, mut stream: SendableRecordBatchStream) -> Result<usize> {
        let (_id, obj_handle) = self.store.put_multipart(&self.loc).await?;
        let mut writer = AsyncBsonWriter::new(obj_handle, BUFFER_SIZE);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write_batch(batch).await?;
        }
        writer.finish().await
    }
}

#[async_trait]
impl DataSink for BsonSink {
    async fn write_all(
        &self,
        data: Vec<SendableRecordBatchStream>,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut count = 0;
        for stream in data {
            count += self
                .stream_into_inner(stream)
                .await
                .map(|x| x as u64)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        Ok(count)
    }
}

/// Wrapper around Arrow's Bson writer to provide async write support.
///
/// Modeled after the parquet crate's `AsyncArrowWriter`.
struct AsyncBsonWriter<W> {
    async_writer: W,
    sync_writer: BsonWriter<SharedBuffer>,
    buffer: SharedBuffer,
    row_count: usize,
}

pub struct BsonWriter<W>
where
    W: Write,
{
    /// Underlying writer to use to write bytes
    writer: W,
}

impl<W> BsonWriter<W>
where
    W: Write,
{
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Convert the `RecordBatch` into BSON rows, and write them to
    /// the output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let coverter = bson::BsonBatchConverter::new(
            StructArray::from(batch.to_owned()),
            batch.schema().fields().to_owned(),
        );

        for doc in coverter {
            doc.to_writer(&mut self.writer)
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
        }

        Ok(())
    }

    /// Convert the [`RecordBatch`] into BSON rows, and write them to
    /// the output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(batch)?;
        }
        Ok(())
    }

    /// Finishes the output stream.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        Ok(())
    }

    /// Unwraps this `Writer<W>`, returning the underlying writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> RecordBatchWriter for BsonWriter<W>
where
    W: Write,
{
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch)
    }

    fn close(mut self) -> Result<(), ArrowError> {
        self.finish()
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncBsonWriter<W> {
    fn new(async_writer: W, buf_size: usize) -> Self {
        let buf = SharedBuffer::with_capacity(buf_size);
        let sync_writer = BsonWriter::new(buf.clone());
        AsyncBsonWriter {
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
