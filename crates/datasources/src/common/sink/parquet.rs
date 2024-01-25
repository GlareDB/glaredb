use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

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

impl fmt::Display for ParquetSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ParquetSink({}:{})", self.store, self.loc)
    }
}

impl DisplayAs for ParquetSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{self}"),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl ParquetSink {
    pub fn from_obj_store(
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

    async fn stream_into_inner(&self, mut stream: SendableRecordBatchStream) -> DfResult<usize> {
        let schema = stream.schema();

        let (_id, obj_handle) = self
            .store
            .put_multipart(&self.loc)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

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
impl DataSink for ParquetSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        self.stream_into_inner(data).await.map(|x| x as u64)
    }
}
