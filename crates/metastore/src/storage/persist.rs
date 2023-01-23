use crate::proto::storage;
use crate::storage::lease::{RemoteLease, RemoteLeaser};
use crate::storage::{CatalogStorageObject, Result, StorageError};
use crate::types::catalog::CatalogState;
use crate::types::storage::PersistedCatalog;
use bytes::BytesMut;
use object_store::ObjectStore;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use uuid::Uuid;

const PERSISTENT_CATALOG_OBJECT: CatalogStorageObject = CatalogStorageObject("catalog");

/// Persistent storage for database catalogs.
#[derive(Debug, Clone)]
pub struct Storage {
    process_id: Uuid,
    store: Arc<dyn ObjectStore>,

    /// The leaser for leasing catalog objects. Note that leases are used to
    /// ensure atomicity when reading _or_ writing the persisted catalog.
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

    /// Read the state of some catalog.
    ///
    /// The catalog must already exist.
    pub async fn read_catalog(&self, db_id: Uuid) -> Result<PersistedCatalog> {
        let lease = self.leaser.acquire(db_id).await?;

        let path = PERSISTENT_CATALOG_OBJECT.visible_object_path(&db_id);
        let bs = self.store.get(&path).await?.bytes().await?;
        let proto = storage::PersistedCatalog::decode(bs)?;

        let catalog: PersistedCatalog = proto.try_into()?;

        // If this errors, it's likely the lease was in an invalid state, so we
        // shouldn't try to return the catalog in such cases.
        lease.drop_lease().await?;

        Ok(catalog)
    }

    // TODO: This will overwrite new catalogs.
    pub async fn write_catalog(&self, db_id: Uuid, catalog: PersistedCatalog) -> Result<()> {
        let proto: storage::PersistedCatalog = catalog.try_into()?;
        let mut bs = BytesMut::new();
        proto.encode(&mut bs)?;

        let lease = self.leaser.acquire(db_id).await?;
        let tmp_path = PERSISTENT_CATALOG_OBJECT.tmp_object_path(&db_id, &self.process_id);
        let visible_path = PERSISTENT_CATALOG_OBJECT.visible_object_path(&db_id);

        self.store.put(&tmp_path, bs.freeze()).await?;

        if !lease.is_valid() {
            return Err(StorageError::LeaseNotValid { db_id });
        }

        self.store.rename(&tmp_path, &visible_path).await?;

        Ok(())
    }
}
