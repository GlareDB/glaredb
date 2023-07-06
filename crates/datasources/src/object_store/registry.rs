use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use url::Url;

#[derive(Debug)]
pub struct GlareDBRegistry;
impl ObjectStoreRegistry for GlareDBRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Option<Arc<dyn object_store::ObjectStore>> {
        println!("register_store: {:?}", url);
        None
    }

    fn get_store(
        &self,
        url: &Url,
    ) -> datafusion::error::Result<Arc<dyn object_store::ObjectStore>> {
        match url.scheme() {
            "s3" => todo!(),
            "http" | "https" => {
                let url = format!("{}://{}", url.scheme(), url.authority());

                let os = object_store::http::HttpBuilder::new()
                    .with_url(&url)
                    .build()?;

                Ok(Arc::new(os))
            }
            _ => todo!(),
        }
    }
}
