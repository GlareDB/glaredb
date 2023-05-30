use crate::object_store::errors::Result;
use crate::sink::{Sink, SinkError};
use async_trait::async_trait;
use datafusion::parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;

const BUFFER_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ParquetSinkOpts {
    pub row_group_size: usize,
}

impl Default for ParquetSinkOpts {
    fn default() -> Self {
        ParquetSinkOpts {
            row_group_size: 122880,
        }
    }
}

/// Writes parquet files to object storage.
#[derive(Debug, Clone)]
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
    opts: ParquetSinkOpts,
}

impl ParquetSink {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        loc: impl Into<ObjectPath>,
        opts: ParquetSinkOpts,
    ) -> ParquetSink {
        ParquetSink {
            store,
            loc: loc.into(),
            opts,
        }
    }

    async fn stream_into_inner(&self, mut stream: SendableRecordBatchStream) -> Result<usize> {
        let schema = stream.schema();

        let (_id, obj_handle) = self.store.put_multipart(&self.loc).await?;

        let props = WriterProperties::builder()
            .set_created_by("GlareDB".to_string())
            .set_max_row_group_size(self.opts.row_group_size)
            .build();

        let mut writer = AsyncArrowWriter::try_new(obj_handle, schema, BUFFER_SIZE, Some(props))?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write(&batch).await?;
        }

        // Calls `shutdown` internally.
        let stats = writer.close().await?;

        Ok(stats.num_rows as usize)
    }
}

#[async_trait]
impl Sink for ParquetSink {
    async fn stream_into(&self, stream: SendableRecordBatchStream) -> Result<usize, SinkError> {
        match self.stream_into_inner(stream).await {
            Ok(n) => Ok(n),
            Err(e) => Err(SinkError::Boxed(Box::new(e))),
        }
    }
}
