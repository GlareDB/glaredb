use crate::cloudsync::CloudSync;
use crate::errors::{internal, Result};
use object_store::{gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, ObjectStore};
use std::path::Path;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct DataStore {
    sync: CloudSync,
    worker_handle: JoinHandle<()>,
}

impl DataStore {
    /// Create a new store that syncs to a GCP object storage bucket.
    pub async fn new_gcp<P: AsRef<Path>>(
        service_account: P,
        bucket: &str,
        cache: P,
    ) -> Result<Self> {
        let service_account = service_account
            .as_ref()
            .to_str()
            .ok_or(internal!("lossy path conversion for service account path"))?;
        let gcp = GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket)
            .with_service_account_path(service_account)
            .build()?;

        Self::with_object_store(gcp, cache).await
    }

    pub async fn new_local<P: AsRef<Path>>(local: P, cache: P) -> Result<Self> {
        let local = LocalFileSystem::new_with_prefix(local)?;
        Self::with_object_store(local, cache).await
    }

    /// Create a new store backed by the provided object store.
    ///
    /// A cloud sync worker will be started in the background.
    pub async fn with_object_store<O: ObjectStore, P: AsRef<Path>>(
        object: O,
        cache: P,
    ) -> Result<Self> {
        let (sync, worker) = CloudSync::new(object, cache.as_ref().to_path_buf());

        let worker_handle = tokio::spawn(worker.run());

        Ok(DataStore {
            sync,
            worker_handle,
        })
    }
}
