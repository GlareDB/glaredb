use crate::compact::Compactor;
use crate::deltacache::DeltaCache;
use crate::errors::{internal, Result};
use bytes::Bytes;
use common::access::{AccessConfig, ObjectStoreKind};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::{path::Path as ObjectPath, ObjectStore};
use object_store_util::temp::TempObjectStore;
use persistence::object_cache::{ObjectStoreCache, DEFAULT_BYTE_RANGE_SIZE};
use std::fs;
use std::sync::Arc;
use tracing::trace;

/// Global resources for accessing data.
#[derive(Debug, Clone)]
pub struct AccessRuntime {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: AccessConfig,
    deltas: Arc<DeltaCache>,
    compactor: Arc<Compactor>,
    store: Arc<dyn ObjectStore>,
    object_path_prefix: ObjectPath,
}

impl AccessRuntime {
    /// Create a new access runtime with the given object store.
    #[tracing::instrument]
    pub async fn new(config: AccessConfig) -> Result<AccessRuntime> {
        use ObjectStoreKind::*;
        let mut store = match config.object_store {
            LocalTemporary => Arc::new(TempObjectStore::new()?) as Arc<dyn ObjectStore>,
            Local {
                ref object_store_path,
            } => {
                trace!(
                    ?object_store_path,
                    "Create local object store path if nessary"
                );
                fs::create_dir_all(object_store_path)?;
                Arc::new(LocalFileSystem::new_with_prefix(object_store_path)?)
                    as Arc<dyn ObjectStore>
            }
            Memory => Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>,
            Gcs {
                ref service_account_path,
                ref bucket_name,
            } => Arc::new(
                GoogleCloudStorageBuilder::new()
                    .with_service_account_path(service_account_path)
                    .with_bucket_name(bucket_name)
                    .build()?,
            ) as Arc<dyn ObjectStore>,
            S3 => unimplemented!(),
        };

        if config.cached {
            store = Arc::new(ObjectStoreCache::new(
                config
                    .cache_path
                    .as_ref()
                    .ok_or_else(|| internal!("No cache path provided"))?
                    .as_path(),
                DEFAULT_BYTE_RANGE_SIZE,
                config
                    .max_object_store_cache_size
                    .ok_or_else(|| internal!("No max cache size provided"))?,
                store,
            )?);
        }

        validate_object_store_permissions(&config.db_name, &store).await?;

        let object_path_prefix = ObjectPath::from(config.db_name.as_str());

        Ok(AccessRuntime {
            inner: Arc::new(Inner {
                config,
                deltas: Arc::new(DeltaCache::new()),
                compactor: Arc::new(Compactor::new(store.clone())),
                store,
                object_path_prefix,
            }),
        })
    }

    /// Returns the object storage prefix of all objects that this database
    /// contains.
    pub fn object_path_prefix(&self) -> &ObjectPath {
        &self.inner.object_path_prefix
    }

    pub fn delta_cache(&self) -> &Arc<DeltaCache> {
        &self.inner.deltas
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.inner.store
    }

    pub fn compactor(&self) -> &Arc<Compactor> {
        &self.inner.compactor
    }

    pub fn config(&self) -> &AccessConfig {
        &self.inner.config
    }
}

async fn validate_object_store_permissions(
    db_name: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let location = format!("{}/test.bin", db_name);
    let location = ObjectPath::parse(location)?;
    let bytes = Bytes::from("Hello world!");
    let range = 0..bytes.len();

    trace!(?store, ?location, ?bytes, "test object store put");
    store.put(&location, bytes.clone()).await?;

    trace!(?store, ?location, ?range, "test object store get_range");
    let result = store.get_range(&location, range).await?;

    if result != bytes {
        return Err(internal!("Object store test failed: data mismatch"));
    }

    trace!(?store, ?location, "test object store delete");
    store.delete(&location).await?;

    trace!(
        ?store,
        "Able to put, get_range and delete from object store"
    );
    Ok(())
}
