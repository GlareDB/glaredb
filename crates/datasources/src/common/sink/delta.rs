use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use deltalake::operations::write::WriteBuilder;
use deltalake::storage::StorageOptions;
use deltalake::DeltaTableConfig;
use futures::StreamExt;
use url::Url;


/// Writes lance files to object storage.
#[derive(Debug, Clone)]
pub struct DeltaSink {
    url: Url,
    store: Arc<dyn object_store::ObjectStore>,
    path: String,
}

impl fmt::Display for DeltaSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeltaSink({}:{})", self.url, &self.path)
    }
}

impl DisplayAs for DeltaSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{self}"),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl DeltaSink {
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        url: Url,
        path: impl Into<String>,
    ) -> Self {
        DeltaSink {
            url,
            store,
            path: path.into(),
        }
    }

    async fn stream_into_inner(&self, stream: SendableRecordBatchStream) -> DfResult<u64> {
        let loc = self
            .url
            .clone()
            .join(&self.path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table = deltalake::DeltaTable::new(
            deltalake::logstore::default_logstore(
                self.store.clone(),
                &loc,
                &StorageOptions::default(),
            ),
            DeltaTableConfig::default(),
        );

        let mut chunks = stream.chunks(32);

        let mut records: usize = 0;

        while let Some(batches) = chunks.next().await {
            let batches: Result<Vec<_>, _> = batches
                .into_iter()
                .map(|r| {
                    let _ = r.as_ref().map(|b| records += b.num_rows());
                    r
                })
                .collect();

            WriteBuilder::new(table.log_store(), table.snapshot().ok().cloned())
                .with_input_batches(batches?.into_iter())
                .await?;
        }


        Ok(records as u64)
    }
}

#[async_trait]
impl DataSink for DeltaSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        self.stream_into_inner(data).await
    }
}
