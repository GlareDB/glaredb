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
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::WriteBuilder;
use deltalake::storage::StorageOptions;
use futures::StreamExt;
use object_store::prefix::PrefixStore;
use url::Url;

use crate::native::access::arrow_to_delta_safe;

/// Writes lance files to object storage.
#[derive(Debug, Clone)]
pub struct DeltaSink {
    url: Url,
    store: Arc<dyn object_store::ObjectStore>,
}

impl fmt::Display for DeltaSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeltaSink({})", self.url)
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
    pub fn new(store: Arc<dyn object_store::ObjectStore>, url: Url) -> Self {
        DeltaSink { url, store }
    }

    async fn stream_into_inner(&self, stream: SendableRecordBatchStream) -> DfResult<u64> {
        // using this prefix store, mirroring the way that we use it
        // in the native module; looks like delta sort of expects the
        // location to show up both in the store and in the
        // logstore. Feels wrong, probably is, but things work as expected.
        let obj_store = PrefixStore::new(self.store.clone(), self.url.path());

        let store = deltalake::logstore::default_logstore(
            Arc::new(obj_store),
            &self.url.clone(),
            &StorageOptions::default(),
        );

        // eventually this should share code
        // with "create_table" in the native module.
        let mut builder = CreateBuilder::new()
            .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
            .with_table_name(
                self.url
                    .to_file_path()
                    .ok()
                    .ok_or_else(|| {
                        DataFusionError::Internal("could not resolve table path".to_string())
                    })?
                    .file_name()
                    .ok_or_else(|| DataFusionError::Internal("missing  table name".to_string()))?
                    .to_os_string()
                    .into_string()
                    .ok()
                    .ok_or_else(|| {
                        DataFusionError::Internal("could not resolve table path".to_string())
                    })?,
            )
            .with_log_store(store.clone());

        // get resolve the schema; eventually this should share code
        // with "create_table" in the native module.
        for field in stream.schema().fields().into_iter() {
            let col = arrow_to_delta_safe(field.data_type())?;
            builder = builder.with_column(
                field.name().clone(),
                col.data_type,
                field.is_nullable(),
                col.metadata,
            );
        }

        let table = builder.await?;

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
