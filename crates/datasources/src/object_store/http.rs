use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::{http::HttpBuilder, path::Path as ObjectStorePath, ObjectMeta, ObjectStore};
use url::Url;

use crate::object_store::{errors::ObjectStoreSourceError, Result};

use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct HttpStoreAccess {
    /// Http(s) URL for the object.
    pub url: Url,
}

impl Display for HttpStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HttpStoreAccess(url: {})", self.url)
    }
}

#[async_trait]
impl ObjStoreAccess for HttpStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        // `ObjectStoreUrl` takes the URL and strips off the path. This doesn't
        // work with Http store since we want a different store for each base
        // domain. (Context: Tried using base domain and adding path but that
        // causes some bugs with setting percent encoded path and since there's
        // no actual benefit of not storing path, storing full URL just works).
        let u = self
            .url
            .to_string()
            // To make path part of URL we make it a '/'.
            .replace('/', "__slash__")
            // TODO: Add more characters which might be invalid for domain.
            .replace('%', "__percent__");

        Ok(ObjectStoreUrl::parse(u)?)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let builder = HttpBuilder::new().with_url(self.url.to_string());
        let build = builder.build()?;
        Ok(Arc::new(build))
    }

    fn path(&self, _location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::default())
    }

    /// Not supported for HTTP. Simply return the meta assuming no-glob.
    async fn list_globbed(
        &self,
        store: Arc<dyn ObjectStore>,
        pattern: &str,
    ) -> Result<Vec<ObjectMeta>> {
        let location = self.path(pattern)?;
        Ok(vec![self.object_meta(store, &location).await?])
    }

    /// Get the object meta from a HEAD request to the url.
    ///
    /// We avoid using object store's `head` method since it does a PROPFIND
    /// request.
    async fn object_meta(
        &self,
        _store: Arc<dyn ObjectStore>,
        location: &ObjectStorePath,
    ) -> Result<ObjectMeta> {
        let res = reqwest::Client::new().head(self.url.clone()).send().await?;
        let len = res.content_length().ok_or(ObjectStoreSourceError::Static(
            "Missing content-length header",
        ))?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: len as usize,
            e_tag: None,
        })
    }
}
