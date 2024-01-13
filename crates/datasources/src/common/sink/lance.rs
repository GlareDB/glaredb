use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::io::writer::FileWriterOptions;
use lance::Dataset;
use object_store::{path::Path as ObjectPath, ObjectStore};

#[derive(Debug, Clone)]
pub struct LanceSinkOpts {
    pub disable_all_column_stats: Option<bool>,
    pub collect_all_column_stats: Option<bool>,
    pub column_stats: Option<Vec<String>>,
    pub url: Option<url::Url>,
}

/// Writes lance files to object storage.
#[derive(Debug, Clone)]
pub struct LanceSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
    opts: LanceSinkOpts,
}

impl Default for LanceSinkOpts {
    fn default() -> Self {
        LanceSinkOpts {
            collect_all_column_stats: Some(true),
            column_stats: None,
            disable_all_column_stats: None,
            url: None,
        }
    }
}

impl fmt::Display for LanceSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceSink({}:{})", self.store, self.loc)
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
    pub fn try_from_obj_store(
        store: Arc<dyn ObjectStore>,
        loc: impl Into<ObjectPath>,
        opts: Option<LanceSinkOpts>,
    ) -> Result<Self, DataFusionError> {
        Ok(LanceSink {
            store,
            loc: loc.into(),
            opts: match opts {
                Some(o) => o,
                None => LanceSinkOpts::default(),
            },
        })
    }

    async fn stream_into_inner(
        &self,
        stream: SendableRecordBatchStream,
        mut ds: Option<Dataset>,
    ) -> DfResult<Option<Dataset>> {
        let mut opts = FileWriterOptions::default();
        if self.opts.collect_all_column_stats.is_some_and(|val| val) {
            opts.collect_stats_for_fields = stream
                .schema()
                .fields
                .iter()
                .enumerate()
                .map(|v| v.0 as i32)
                .collect();
        } else if self.opts.column_stats.is_some() {
            let colls: &Vec<String> = self.opts.column_stats.as_ref().unwrap();
            let mut set = HashSet::with_capacity(colls.len());

            for c in colls.iter() {
                set.replace(c);
            }

            opts.collect_stats_for_fields = stream
                .schema()
                .fields
                .iter()
                .map(|f| f.name().to_owned())
                .filter(|f| set.contains(f))
                .enumerate()
                .map(|v| v.0 as i32)
                .collect();
        }
        let table = if self.opts.url.is_some() {
            self.opts
                .url
                .clone()
                .unwrap()
                .join(self.loc.as_ref())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            url::Url::parse(self.loc.as_ref())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        let schema = stream.schema().clone();
        let mut chunks = stream.chunks(32);
        let write_opts = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };
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
    async fn write_all(
        &self,
        data: Vec<SendableRecordBatchStream>,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut ds: Option<Dataset> = None;
        for stream in data {
            ds = self.stream_into_inner(stream, ds).await?;
        }
        match ds {
            Some(ds) => Ok(ds.count_rows().await? as u64),
            None => Ok(0),
        }
    }
}
