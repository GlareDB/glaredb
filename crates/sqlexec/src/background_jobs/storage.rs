//! Background jobs for storage tracking.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use datasources::native::access::NativeTableStorage;
use metastore_client::types::service::{Mutation, UpdateDeploymentStorage};
use tokio::time::Instant;

use crate::{errors::Result, metastore::SupervisorClient};

use super::BgJob;

#[derive(Debug)]
pub struct BackgroundJobStorageTracker {
    native_store: NativeTableStorage,
    metastore: SupervisorClient,
}

impl BackgroundJobStorageTracker {
    pub fn new(native_store: NativeTableStorage, metastore: SupervisorClient) -> Arc<Self> {
        Arc::new(Self {
            native_store,
            metastore,
        })
    }
}

#[async_trait]
impl BgJob for BackgroundJobStorageTracker {
    fn name(&self) -> String {
        format!("storage_tracker_{}", self.native_store.db_id())
    }

    fn start_at(&self) -> Instant {
        // Start after 5 minutes of scheduling the job, so we can batch jobs for
        // frequently updating tables.
        Instant::now() + Duration::from_secs(5 * 60)
    }

    async fn start(&self) -> Result<()> {
        let total_size = self.native_store.calculate_db_size().await?;

        // Update the storage size in metastore.
        self.metastore.refresh_cached_state().await?;
        let state = self.metastore.get_cached_state().await?;
        self.metastore
            .try_mutate(
                state.version,
                vec![Mutation::UpdateDeploymentStorage(UpdateDeploymentStorage {
                    new_storage_size: total_size as u64,
                })],
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datasources::native::access::NativeTableStorage;
    use metastore::local::start_inprocess_inmemory;
    use metastore_client::types::{
        catalog::{EntryMeta, EntryType, TableEntry},
        options::{InternalColumnDefinition, TableOptions, TableOptionsInternal},
    };
    use object_store_util::conf::StorageConfig;
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::{
        background_jobs::JobRunner,
        metastore::{Supervisor, DEFAULT_WORKER_CONFIG},
    };

    use super::BackgroundJobStorageTracker;

    #[tokio::test]
    async fn test_background_job_storage_tracker() {
        let db_id = Uuid::new_v4();
        let dir = tempdir().unwrap();

        let storage = NativeTableStorage::from_config(
            db_id,
            StorageConfig::Local {
                path: dir.path().to_path_buf(),
            },
        )
        .unwrap();

        // Add some tables inside the temp dir to get a non-zero storage size.
        storage
            .create_table(&TableEntry {
                meta: EntryMeta {
                    entry_type: EntryType::Table,
                    id: 12345,
                    parent: 54321,
                    name: "table_1".to_string(),
                    builtin: false,
                    external: false,
                },
                options: TableOptions::Internal(TableOptionsInternal {
                    columns: vec![InternalColumnDefinition {
                        name: "id".to_string(),
                        nullable: true,
                        arrow_type: DataType::Int32,
                    }],
                }),
                tunnel_id: None,
            })
            .await
            .unwrap();

        let meta_chan = start_inprocess_inmemory().await.unwrap();
        let metastore = Supervisor::new(meta_chan, DEFAULT_WORKER_CONFIG);
        let metastore = metastore.init_client(Uuid::new_v4(), db_id).await.unwrap();

        let tracker = BackgroundJobStorageTracker::new(storage, metastore.clone());
        let jobs = JobRunner::new(Default::default());
        jobs.add(tracker).unwrap();
        jobs.close().await.unwrap();

        // Check if data exists in metastore.
        metastore.refresh_cached_state().await.unwrap();
        let state = metastore.get_cached_state().await.unwrap();
        assert_ne!(state.deployment.storage_size, 0);
    }
}
