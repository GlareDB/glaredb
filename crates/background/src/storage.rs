use crate::errors::Result;
use access::runtime::AccessRuntime;
use cloud::client::CloudClient;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use std::sync::Arc;
use tracing::debug;

/// Background job for computing total storage usage of this database.
#[derive(Debug)]
pub struct DatabaseStorageUsageJob {
    runtime: Arc<AccessRuntime>,
    /// Prefix of all database objects in object store.
    prefix: ObjectPath,
}

impl DatabaseStorageUsageJob {
    /// Create a new worker for computing storage usage.
    pub fn new(runtime: Arc<AccessRuntime>) -> Self {
        let prefix = runtime.object_store_path_prefix();
        DatabaseStorageUsageJob { runtime, prefix }
    }

    /// Compute the total storage in bytes that this database is taking up in
    /// object store.
    pub async fn compute_storage_total_bytes(&self) -> Result<u64> {
        debug!(prefix = %self.prefix, "computing storage usage with prefix");
        let stream = self.runtime.object_store().list(Some(&self.prefix)).await?;
        let total = stream
            .try_fold(0, |acc, meta| async move { Ok(acc + meta.size) })
            .await?;
        Ok(total as u64)
    }
}

/// Where to send storage usage.
#[derive(Debug)]
pub struct DatabaseStorageUsageSink {
    client: Option<Arc<CloudClient>>,
}

impl DatabaseStorageUsageSink {
    /// Create a new sink. If client is `None`, no attempt will be made to
    /// report to cloud.
    pub fn new(client: Option<Arc<CloudClient>>) -> Self {
        DatabaseStorageUsageSink { client }
    }

    /// Send storage usage to cloud if available.
    pub async fn send_usage(&self, usage_bytes: u64) -> Result<()> {
        match &self.client {
            Some(client) => {
                client.report_usage(usage_bytes).await?;
            }
            None => {
                debug!("skipping sending storage usage to Cloud");
            }
        }
        Ok(())
    }
}
