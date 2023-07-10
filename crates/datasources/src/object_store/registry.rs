use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreRegistry};
use metastoreproto::{
    session::NamespacedCatalogEntry,
    types::options::{TableOptionsGcs, TableOptionsS3},
};
use object_store::{local::LocalFileSystem, ObjectStore};
use url::Url;

use crate::object_store::{errors::Result, gcs::GcsTableAccess, s3::S3TableAccess};

#[derive(Debug)]
pub struct GlareDBRegistry {
    /// Object stores keyed by scheme and bucket name.
    ///
    /// Note that this means we currently only support one set of credentials
    /// per bucket.
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
}

impl GlareDBRegistry {
    /// Create a new registry for a set of catalog entries.
    pub fn try_new<'a>(entries: impl Iterator<Item = NamespacedCatalogEntry<'a>>) -> Result<Self> {
        use metastoreproto::types::catalog::CatalogEntry;
        use metastoreproto::types::options::TableOptions;
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        object_stores.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        for entry in entries {
            if let CatalogEntry::Table(tbl_entry) = entry.entry {
                match &tbl_entry.options {
                    TableOptions::Gcs(opts) => {
                        let store = gcs_to_store(opts)?;
                        let key = format!("gs://{}", opts.bucket);
                        object_stores.insert(key, store);
                    }
                    TableOptions::S3(opts) => {
                        let store = s3_to_store(opts)?;
                        let key = format!("s3://{}", opts.bucket);
                        object_stores.insert(key, store);
                    }
                    _ => (), // TODO: Add azure when supported.
                }
            };
        }
        Ok(Self { object_stores })
    }
}

fn gcs_to_store(opts: &TableOptionsGcs) -> Result<Arc<dyn ObjectStore>> {
    let (store, _) = GcsTableAccess {
        bucket_name: opts.bucket.clone(),
        service_acccount_key_json: opts.service_account_key.clone(),
        location: opts.location.clone(),
        file_type: None,
    }
    .store_and_path()?;
    Ok(store)
}

fn s3_to_store(opts: &TableOptionsS3) -> Result<Arc<dyn ObjectStore>> {
    let (store, _) = S3TableAccess {
        region: opts.region.clone(),
        bucket_name: opts.bucket.clone(),
        access_key_id: opts.access_key_id.clone(),
        secret_access_key: opts.secret_access_key.clone(),
        location: opts.location.clone(),
        file_type: None,
    }
    .store_and_path()?;
    Ok(store)
}

impl ObjectStoreRegistry for GlareDBRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        self.object_stores.insert(s, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let s = get_url_key(url);
        self.object_stores
            .get(&s)
            .map(|o| o.value().clone())
            .or_else(|| {
                if matches!(url.scheme(), "http" | "https") {
                    let store = object_store::http::HttpBuilder::new()
                        .with_url(url.as_str())
                        .build()
                        .unwrap();
                    Some(Arc::new(store))
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                DataFusionError::Internal(format!("No suitable object store found for {url}"))
            })
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}
