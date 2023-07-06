use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreRegistry};
use object_store::ObjectStore;
use url::Url;

#[derive(Debug)]
pub struct GlareDBRegistry {
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
}

impl Default for GlareDBRegistry {
    fn default() -> Self {
        Self::new()
    }
}
impl GlareDBRegistry {
    pub fn new() -> Self {
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();

        Self { object_stores }
    }
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
