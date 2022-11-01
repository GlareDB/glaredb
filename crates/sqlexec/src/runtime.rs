use crate::errors::{internal, Result};
use access::compact::Compactor;
use access::deltacache::DeltaCache;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use object_store_util::temp::TempObjectStore;
use persistence::object_cache::{ObjectStoreCache, DEFAULT_BYTE_RANGE_SIZE};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

//TODO: Update these so they are from a config file rather than env variables
const GCS_SERVICE_ACCOUNT_PATH: &str = "GCS_SERVICE_ACCOUNT_PATH";
const BUCKET_NAME: &str = "GCS_BUCKET_NAME";

#[derive(Debug, Default)]
pub enum ObjectStoreKind {
    #[default]
    LocalTemp,
    Memory,
    Gcs {
        service_account_path: String,
        bucket_name: String,
    },
    S3,
}

impl FromStr for ObjectStoreKind {
    type Err = crate::errors::ExecError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ObjectStoreKind::*;
        match s {
            "local" => Ok(LocalTemp),
            "memory" => Ok(Memory),
            "gcs" => {
                let service_account_path =
                    std::env::var(GCS_SERVICE_ACCOUNT_PATH).map_err(|e| {
                        internal!(
                            "GCS: {} environment variable error: {:?}",
                            GCS_SERVICE_ACCOUNT_PATH,
                            e
                        )
                    })?;
                let bucket_name = std::env::var(BUCKET_NAME).map_err(|e| {
                    internal!("GCS: {} environment variable error: {:?}", BUCKET_NAME, e)
                })?;
                Ok(Gcs {
                    service_account_path,
                    bucket_name,
                })
            }
            _ => Err(internal!(
                "This type of object storage is not supported: {}",
                s
            )),
        }
    }
}

//TODO: use new default, better yet ensure everything is set in config file
#[derive(Debug, Default)]
pub struct AccessConfig {
    pub object_store: ObjectStoreKind,
    pub cached: bool,
    pub max_object_store_cache_size: Option<u64>,
    pub cache_path: Option<PathBuf>,
}

/// Global resources for accessing data.
#[derive(Debug, Clone)]
pub struct AccessRuntime {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    conf: AccessConfig,
    deltas: Arc<DeltaCache>,
    compactor: Arc<Compactor>,
    store: Arc<dyn ObjectStore>,
}

impl AccessRuntime {
    /// Create a new access runtime with the given object store.
    pub fn new(config: AccessConfig) -> Result<AccessRuntime> {
        use ObjectStoreKind::*;
        let mut store = match config.object_store {
            LocalTemp => Arc::new(TempObjectStore::new()?) as Arc<dyn ObjectStore>,
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

        Ok(AccessRuntime {
            inner: Arc::new(Inner {
                conf: config,
                deltas: Arc::new(DeltaCache::new()),
                compactor: Arc::new(Compactor::new(store.clone())),
                store,
            }),
        })
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
        &self.inner.conf
    }
}
