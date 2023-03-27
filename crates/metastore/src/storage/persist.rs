use crate::proto::storage;
use crate::storage::lease::{RemoteLease, RemoteLeaser};
use crate::storage::{
    Result, SingletonStorageObject, StorageError, StorageObject, VersionedStorageObject,
};
use crate::types::catalog::CatalogState;
use crate::types::storage::{CatalogMetadata, ExtraState, PersistedCatalog};
use bytes::BytesMut;
use object_store::{Error as ObjectStoreError, ObjectStore};
use pgrepr::oid::FIRST_AVAILABLE_ID;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};
use uuid::Uuid;

/// The metadata object for the catalog.
const CATALOG_METADATA: SingletonStorageObject = SingletonStorageObject("metadata");

const PERSISTENT_CATALOG_OBJECT: VersionedStorageObject = VersionedStorageObject("catalog", 0);

/// Persistent storage for database catalogs.
#[derive(Debug, Clone)]
pub struct Storage {
    process_id: Uuid,
    store: Arc<dyn ObjectStore>,

    /// The leaser for leasing catalog objects. Leases are only used when making
    /// modifications to the catalog.
    leaser: RemoteLeaser,
}

impl Storage {
    pub fn new(process_id: Uuid, store: Arc<dyn ObjectStore>) -> Storage {
        let leaser = RemoteLeaser::new(process_id, store.clone());
        Storage {
            process_id,
            store,
            leaser,
        }
    }

    /// Initialize a new catalog for a database.
    ///
    /// Idempotent via checking if metadata for the catalog exists. If
    /// initialization fails, the state of the catalog is not guaranteed, but
    /// subsequent calls to this function should fix catalogs that haven't been
    /// fully initialized.
    ///
    /// Once initialized, a catalog is ready for use without further
    /// modification. The OID counter is guaranteed to be valid.
    pub async fn initialize(&self, db_id: Uuid) -> Result<()> {
        match self
            .store
            .head(&CATALOG_METADATA.visible_path(&db_id))
            .await
        {
            Ok(_) => return Ok(()),                       // Nothing to do.
            Err(ObjectStoreError::NotFound { .. }) => (), // Continue on creating new catalog.
            Err(e) => return Err(e.into()),               // Something else happened...
        }

        info!(%db_id, "initializing new catalog for database");

        // Create the new catalog. This is always safe to write directly to
        // visible since the content should always be the same.
        let first_catalog: storage::PersistedCatalog = PersistedCatalog {
            state: CatalogState {
                version: 0,
                entries: HashMap::new(),
            },
            extra: ExtraState {
                oid_counter: FIRST_AVAILABLE_ID,
            },
        }
        .try_into()?;
        let mut bs = BytesMut::new();
        first_catalog.encode(&mut bs)?;

        // Write version 0 of the catalog...
        self.store
            .put(&PERSISTENT_CATALOG_OBJECT.visible_path(&db_id), bs.freeze())
            .await?;

        // Initialize lease.
        self.leaser.initialize(&db_id).await?;

        // And immediately acquire.
        let lease = self.leaser.acquire(db_id).await?;

        // Write first metadata.
        let first_metadata: storage::CatalogMetadata = CatalogMetadata {
            latest_version: 0,
            last_written_by: self.process_id,
        }
        .into();
        let mut bs = BytesMut::new();
        first_metadata.encode(&mut bs)?;

        self.store
            .put(&CATALOG_METADATA.visible_path(&db_id), bs.freeze())
            .await?;

        lease.drop_lease().await?;

        Ok(())
    }

    pub async fn latest_version(&self, db_id: &Uuid) -> Result<u64> {
        Ok(self.read_metadata(db_id).await?.latest_version)
    }

    /// Read the state of some catalog.
    ///
    /// The catalog must already exist.
    pub async fn read_catalog(&self, db_id: Uuid) -> Result<PersistedCatalog> {
        // Note that we're not acquiring a lease. These reads are safe since
        // we're not (currently) removing old catalog versions. And at most,
        // we'll be reading one version out of date.

        let metadata = self.read_metadata(&db_id).await?;

        let path = PERSISTENT_CATALOG_OBJECT
            .with_version(metadata.latest_version)
            .visible_path(&db_id);
        let bs = self.store.get(&path).await?.bytes().await?;

        // Log we'll want to keep an eye on so we can monitor catalog size.
        debug!(byte_len = %bs.len(), %db_id, "read catalog");

        let proto = storage::PersistedCatalog::decode(bs)?;

        Ok(proto.try_into()?)
    }

    /// Write a new version of the catalog.
    ///
    /// The catalog must already exist.
    pub async fn write_catalog(
        &self,
        db_id: Uuid,
        old_version: u64,
        catalog: PersistedCatalog,
    ) -> Result<()> {
        // Unlike reads, writes need to acquire the lease for the catalog.
        //
        // The steps are as follows:
        // 1. Acquire catalog lease.
        // 2. Read metadata.
        //   - Error if modifications were made to out of date catalog.
        // 3. Write versioned catalog to temp space.
        // 4. Write new metadata to temp space.
        // 5. Rename temp catalog to make visible.
        // 6. Rename metadata to make visible.
        //
        // Only after step 6 will the new version of the catalog be read.
        //
        // Note that this relies heavily on the lease working correctly to
        // prevent multiple processes writing at the same time.

        let lease = self.leaser.acquire(db_id).await?;

        // Steps 2 through 6...
        if let Err(e) = self
            .write_catalog_inner(db_id, old_version, catalog, &lease)
            .await
        {
            if let Err(e) = lease.drop_lease().await {
                error!(%e, "failed to drop lease after failing to write catalog");
                // Continue on, want to return the original error.
            }
            return Err(e);
        }

        // Drop after successful write.
        lease.drop_lease().await?;

        Ok(())
    }

    async fn write_catalog_inner(
        &self,
        db_id: Uuid,
        old_version: u64,
        catalog: PersistedCatalog,
        lease: &RemoteLease,
    ) -> Result<()> {
        let metadata = self.read_metadata(&db_id).await?;

        if metadata.latest_version != old_version {
            return Err(StorageError::AttemptedOutOfDataCatalogWrite {
                expected: metadata.latest_version,
                have: old_version,
            });
        }

        // New metadata to write.
        let metadata = CatalogMetadata {
            latest_version: catalog.state.version,
            last_written_by: self.process_id,
        };

        let catalog_obj = PERSISTENT_CATALOG_OBJECT.with_version(catalog.state.version);

        let proto: storage::PersistedCatalog = catalog.try_into()?;
        let mut bs = BytesMut::new();
        proto.encode(&mut bs)?;

        let tmp_catalog_path = catalog_obj.tmp_path(&db_id, &self.process_id);
        self.store.put(&tmp_catalog_path, bs.freeze()).await?;

        let proto: storage::CatalogMetadata = metadata.into();
        let mut bs = BytesMut::new();
        proto.encode(&mut bs)?;

        let tmp_metadata_path = CATALOG_METADATA.tmp_path(&db_id, &self.process_id);
        self.store.put(&tmp_metadata_path, bs.freeze()).await?;

        // Move objects...

        // Blindly overwrite to prevent subsequent catalog writes from getting
        // stuck if we happen to fail on step 6 or fail the lease check.
        self.store
            .rename(&tmp_catalog_path, &catalog_obj.visible_path(&db_id))
            .await?;

        // Last chance to bail before attempting to make our changes visible.
        if !lease.is_valid() {
            return Err(StorageError::LeaseNotValid { db_id });
        }

        self.store
            .rename(&tmp_metadata_path, &CATALOG_METADATA.visible_path(&db_id))
            .await?;

        Ok(())
    }

    /// Read the metadata for a catalog.
    ///
    /// Note that this doesn't require a lease if the catalog is only being
    /// read, as reads from object store are atomic. Reading a metadata object
    /// that in the process of being written to will just mean we'll read a
    /// catalog that's one version out of date. _Technically_ this is
    /// serializable since we would error on any attempts to mutate the catalog
    /// with that version.
    async fn read_metadata(&self, db_id: &Uuid) -> Result<CatalogMetadata> {
        let path = CATALOG_METADATA.visible_path(db_id);
        let bs = self.store.get(&path).await?.bytes().await?;
        let proto = storage::CatalogMetadata::decode(bs)?;

        Ok(proto.try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn new_storage() -> Storage {
        let process_id = Uuid::new_v4();
        let store = Arc::new(InMemory::new());
        let leaser = RemoteLeaser::new(process_id, store.clone());
        Storage {
            process_id,
            store,
            leaser,
        }
    }

    #[tokio::test]
    async fn initialize_idempotent() {
        let storage = new_storage();

        let db_id = Uuid::new_v4();
        storage.initialize(db_id).await.unwrap();
        storage.initialize(db_id).await.unwrap();
    }

    #[tokio::test]
    async fn write_simple() {
        let storage = new_storage();

        let db_id = Uuid::new_v4();
        storage.initialize(db_id).await.unwrap();

        let mut catalog = storage.read_catalog(db_id).await.unwrap();

        let old_version = catalog.state.version;
        catalog.state.version += 1;
        storage
            .write_catalog(db_id, old_version, catalog.clone())
            .await
            .unwrap();

        let updated = storage.read_catalog(db_id).await.unwrap();
        assert_eq!(1, updated.state.version);

        // Check that we can't write using out of date version.
        storage.write_catalog(db_id, 0, catalog).await.unwrap_err();
    }

    #[tokio::test]
    async fn write_failed_lease() {
        let storage = new_storage();

        let db_id = Uuid::new_v4();
        storage.initialize(db_id).await.unwrap();

        // Sneakily get lease for catalog.
        let lease = storage.leaser.acquire(db_id).await.unwrap();

        // Reads should work.
        let mut catalog = storage.read_catalog(db_id).await.unwrap();

        let old_version = catalog.state.version;
        catalog.state.version += 1;
        // Write should fail, can't acquire lease.
        storage
            .write_catalog(db_id, old_version, catalog.clone())
            .await
            .unwrap_err();

        // Write should work after dropping lease.
        lease.drop_lease().await.unwrap();
        storage
            .write_catalog(db_id, old_version, catalog)
            .await
            .unwrap();
    }
}
