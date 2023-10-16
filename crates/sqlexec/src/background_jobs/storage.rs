//! Background jobs for storage tracking.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use datasources::native::access::NativeTableStorage;
use protogen::metastore::types::{
    catalog::TableEntry,
    service::{Mutation, UpdateDeploymentStorage},
};
use tokio::time::Instant;

use crate::{
    errors::{ExecError, Result},
    metastore::catalog::CatalogMutator,
};

use super::BgJob;

#[derive(Debug)]
pub struct BackgroundJobStorageTracker {
    native_store: NativeTableStorage,
    metastore: CatalogMutator,
}

impl BackgroundJobStorageTracker {
    #[allow(dead_code)]
    pub fn new(native_store: NativeTableStorage, metastore: CatalogMutator) -> Arc<Self> {
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
        let catalog_version = match self.metastore.get_metastore_client() {
            Some(client) => {
                client.refresh_cached_state().await?;
                let latest_state = client.get_cached_state().await?;
                latest_state.version
            }
            None => {
                return Err(ExecError::Internal(
                    "cannot get latest catalog version: client missing".to_string(),
                ));
            }
        };

        self.metastore
            .mutate(
                catalog_version,
                [Mutation::UpdateDeploymentStorage(UpdateDeploymentStorage {
                    new_storage_size: total_size as u64,
                })],
            )
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct BackgroundJobDeleteTable {
    native_store: NativeTableStorage,
    table_entry: TableEntry,
}

impl BackgroundJobDeleteTable {
    #[allow(dead_code)]
    pub fn new(native_store: NativeTableStorage, table_entry: TableEntry) -> Arc<Self> {
        Arc::new(Self {
            native_store,
            table_entry,
        })
    }
}

#[async_trait]
impl BgJob for BackgroundJobDeleteTable {
    fn name(&self) -> String {
        format!(
            "delete_table_{}_{}",
            self.native_store.db_id(),
            self.table_entry.meta.name
        )
    }

    fn start_at(&self) -> Instant {
        // schedule the delete task to run immediately
        Instant::now()
    }

    async fn start(&self) -> Result<()> {
        self.native_store.delete_table(&self.table_entry).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datasources::native::access::{NativeTableStorage, SaveMode};
    use metastore::local::start_inprocess_inmemory;
    use protogen::metastore::types::{
        catalog::{EntryMeta, EntryType, TableEntry},
        options::{InternalColumnDefinition, TableOptions, TableOptionsInternal},
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::engine::EngineStorageConfig;
    use crate::{
        background_jobs::JobRunner,
        metastore::{
            catalog::CatalogMutator,
            client::{MetastoreClientSupervisor, DEFAULT_METASTORE_CLIENT_CONFIG},
        },
    };

    use super::BackgroundJobStorageTracker;

    #[tokio::test]
    async fn test_background_job_storage_tracker() {
        let db_id = Uuid::new_v4();
        let dir = tempdir().unwrap();
        let conf = EngineStorageConfig::try_from_path_buf(&dir.path().to_path_buf()).unwrap();

        let storage = NativeTableStorage::new(db_id, conf.url(), conf.new_object_store().unwrap());

        // Add some tables inside the temp dir to get a non-zero storage size.
        storage
            .create_table(
                &TableEntry {
                    meta: EntryMeta {
                        entry_type: EntryType::Table,
                        id: 12345,
                        parent: 54321,
                        name: "table_1".to_string(),
                        builtin: false,
                        external: false,
                        is_temp: false,
                    },
                    options: TableOptions::Internal(TableOptionsInternal {
                        columns: vec![InternalColumnDefinition {
                            name: "id".to_string(),
                            nullable: true,
                            arrow_type: DataType::Int32,
                        }],
                    }),
                    tunnel_id: None,
                },
                SaveMode::ErrorIfExists,
            )
            .await
            .unwrap();

        let meta_chan = start_inprocess_inmemory().await.unwrap();
        let metastore = MetastoreClientSupervisor::new(meta_chan, DEFAULT_METASTORE_CLIENT_CONFIG);
        let metastore = metastore.init_client(db_id).await.unwrap();
        let mutator = CatalogMutator::new(Some(metastore.clone()));

        let tracker = BackgroundJobStorageTracker::new(storage, mutator);
        let jobs = JobRunner::new(Default::default());
        jobs.add(tracker).unwrap();
        jobs.close().await.unwrap();

        // Check if data exists in metastore.
        metastore.refresh_cached_state().await.unwrap();
        let state = metastore.get_cached_state().await.unwrap();
        assert_ne!(state.deployment.storage_size, 0);
    }
}
