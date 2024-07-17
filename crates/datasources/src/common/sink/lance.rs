use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use lance::dataset::WriteMode;
use lance::Dataset;
use object_store::path::Path as ObjectPath;

pub type LanceWriteParams = lance::dataset::WriteParams;

#[derive(Debug, Clone)]
pub struct LanceSinkOpts {
    pub url: Option<url::Url>,
    pub max_rows_per_file: usize,
    pub max_rows_per_group: usize,
    pub max_bytes_per_file: usize,
    pub input_batch_size: usize,
}

/// Writes lance files to object storage.
#[derive(Debug, Clone)]
pub struct LanceSink {
    loc: ObjectPath,
    opts: LanceSinkOpts,
}

impl fmt::Display for LanceSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceSink({})", self.loc)
    }
}

impl DisplayAs for LanceSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{self}"),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl LanceSink {
    pub fn from_obj_store(loc: impl Into<ObjectPath>, opts: LanceSinkOpts) -> Self {
        LanceSink {
            loc: loc.into(),
            opts,
        }
    }

    async fn stream_into_inner(
        &self,
        stream: SendableRecordBatchStream,
    ) -> DfResult<Option<Dataset>> {
        let table = match self.opts.url.clone() {
            Some(opts_url) => opts_url.join(self.loc.as_ref()),
            None => url::Url::parse(self.loc.as_ref()),
        }
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema = stream.schema().clone();
        let mut chunks = stream.chunks(32);
        let write_opts = LanceWriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };


        let mut ds: Option<Dataset> = None;

        while let Some(batches) = chunks.next().await {
            let batch_iter =
                RecordBatchIterator::new(batches.into_iter().map(|item| Ok(item?)), schema.clone());

            match ds.clone() {
                Some(mut d) => {
                    d.append(batch_iter, Some(write_opts.clone())).await?;
                }
                None => {
                    ds.replace(
                        Dataset::write(batch_iter, table.as_str(), Some(write_opts.clone()))
                            .await?,
                    );
                }
            }
        }

        Ok(ds)
    }
}

#[async_trait]
impl DataSink for LanceSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    // the dataset is the handle to the lance database.
    //
    // there's no way to construct an empty dataset except by writing
    // to it, so we pass this optional wrapped dataset to this method,
    // if it's none, we create a new one, and if it's not we use the
    // dataset we constructed before from the optional, and return it,
    // and pass it into the next call.
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        Ok(match self.stream_into_inner(data).await? {
            Some(ds) => ds.count_rows().await? as u64,
            None => 0,
        })
    }
}
