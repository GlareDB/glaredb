use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreRegistry};
use metastore_client::types::options::TableOptions;
use object_store::ObjectStore;
use url::Url;

use crate::object_store::{
    errors::Result, gcs::GcsStoreAccess, local::LocalStoreAccess, s3::S3StoreAccess, ObjStoreAccess,
};

/// The custome registry is needed to map dynamic urls to object stores (such as the case with HTTP).
#[derive(Debug)]
pub struct GlareDBRegistry {
    /// Object stores keyed by scheme and bucket name.
    ///
    /// Note that this means we currently only support one set of credentials
    /// per bucket.
    object_stores: DashMap<Url, Arc<dyn ObjectStore>>,
}

impl GlareDBRegistry {
    /// Create a new registry for a set of catalog entries.
    pub fn try_new<'a>(entries: impl Iterator<Item = &'a TableOptions>) -> Result<Self> {
        let object_stores: DashMap<Url, Arc<dyn ObjectStore>> = DashMap::new();

        for opts in entries {
            let access: Arc<dyn ObjStoreAccess> = match opts {
                TableOptions::Local(_) => Arc::new(LocalStoreAccess),
                TableOptions::Gcs(opts) => Arc::new(GcsStoreAccess {
                    bucket: opts.bucket.clone(),
                    service_account_key: opts.service_account_key.clone(),
                }),
                TableOptions::S3(opts) => Arc::new(S3StoreAccess {
                    region: opts.region.clone(),
                    bucket: opts.bucket.clone(),
                    access_key_id: opts.access_key_id.clone(),
                    secret_access_key: opts.secret_access_key.clone(),
                }),
                // Continue on all others. Explicityly mentioning all the left
                // over options so we don't forget adding object stores that are
                // supported in the furure (like azure).
                TableOptions::Internal(_)
                | TableOptions::Debug(_)
                | TableOptions::Postgres(_)
                | TableOptions::BigQuery(_)
                | TableOptions::Mysql(_)
                | TableOptions::Mongo(_)
                | TableOptions::Snowflake(_) => continue,
            };

            let base_url = access.base_url()?;
            let url: &Url = base_url.as_ref();
            let store = access.create_store()?;
            object_stores.insert(url.clone(), store);
        }

        Ok(Self { object_stores })
    }
}

impl ObjectStoreRegistry for GlareDBRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.object_stores.insert(url.clone(), store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        self.object_stores
            .get(url)
            .map(|o| o.value().clone())
            .or_else(|| match url.scheme() {
                "http" | "https" => {
                    let store = object_store::http::HttpBuilder::new()
                        .with_url(url.as_str())
                        .build()
                        .unwrap();
                    Some(Arc::new(store))
                }
                "file" => {
                    let store = object_store::local::LocalFileSystem::new();
                    Some(Arc::new(store))
                }
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::External(
                    format!(
                        "------\n
                    No  suitable object store found for {url}"
                    )
                    .into(),
                )
            })
    }
}
