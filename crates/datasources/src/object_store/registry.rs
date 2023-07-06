use std::sync::Arc;

use dashmap::DashMap;
use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::{local::LocalFileSystem, ObjectStore};
use url::Url;

#[derive(Debug)]
pub struct GlareDBRegistry {
    object_stores: DashMap<Url, Arc<dyn ObjectStore>>,
}

impl Default for GlareDBRegistry {
    fn default() -> Self {
        Self::new()
    }
}
impl GlareDBRegistry {
    pub fn new() -> Self {
        let object_stores: DashMap<Url, Arc<dyn ObjectStore>> = DashMap::new();

        Self { object_stores }
    }
}

impl ObjectStoreRegistry for GlareDBRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        println!("register_store for : {}", url);
        self.object_stores.insert(url.clone(), store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.object_stores.get(url) {
            println!("found store for : {}", url);

            return Ok(store.clone());
        } else {
            match url.scheme() {
                "s3" => {
                    let store = object_store::aws::AmazonS3Builder::new()
                        .with_url(url.as_str())
                        .build()?;
                    Ok(Arc::new(store))
                }
                "gs" => {
                    let store = object_store::gcp::GoogleCloudStorageBuilder::new()
                        .with_url(url.as_str())
                        .build()?;
                    Ok(Arc::new(store))
                }

                "http" | "https" => {
                    let os = object_store::http::HttpBuilder::new()
                        .with_url(url.as_str())
                        .build()?;

                    Ok(Arc::new(os))
                }
                "file" => Ok(Arc::new(LocalFileSystem::new())),
                _ => {
                    todo!()
                }
            }
        }
    }
}
