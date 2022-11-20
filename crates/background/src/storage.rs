use crate::errors::{BackgroundError, Result};
use access::runtime::AccessRuntime;
use futures::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;

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
    async fn compute_storage_total_bytes(&self) -> Result<u64> {
        let stream = self.runtime.object_store().list(Some(&self.prefix)).await?;
        let total = stream
            .try_fold(0, |acc, meta| async move { Ok(acc + meta.size) })
            .await?;
        Ok(total as u64)
    }
}
