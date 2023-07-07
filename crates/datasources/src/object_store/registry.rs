use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreRegistry};
use metastoreproto::{session::NamespacedCatalogEntry, types::options::TableOptionsGcs};
use object_store::{local::LocalFileSystem, ObjectStore};
use url::Url;

use crate::object_store::gcs::GcsTableAccess;

#[derive(Debug)]
pub struct GlareDBRegistry {
    // catalog: Vec<NamespacedCatalogEntry<'a>>,
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
}

impl GlareDBRegistry {
    pub fn new<'a>(entries: impl Iterator<Item = NamespacedCatalogEntry<'a>>) -> Self {
        use metastoreproto::types::catalog::CatalogEntry;
        use metastoreproto::types::options::TableOptions;
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        object_stores.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        for entry in entries {
            match entry.entry {
                CatalogEntry::Table(tbl_entry) => match &tbl_entry.options {
                    TableOptions::Gcs(opts) => {
                        let store = gcs_to_store(opts);
                        let key = format!("gs://{}", opts.bucket);

                        object_stores.insert(key, store);
                    }
                    _ => todo!("add support for other object stores"),
                },
                _ => todo!("add support for other object stores"),
            };
        }
        Self { object_stores }
    }
}

fn gcs_to_store(opts: &TableOptionsGcs) -> Arc<dyn ObjectStore> {
    GcsTableAccess {
        bucket_name: opts.bucket.clone(),
        service_acccount_key_json: opts.service_account_key.clone(),
        location: opts.location.clone(),
        file_type: None,
    }
    .into_object_store()
    .unwrap()
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
