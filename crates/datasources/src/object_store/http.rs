use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::FileFormat;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use url::Url;

use super::glob_util::ResolvedPattern;
use super::ObjStoreAccess;
use crate::object_store::errors::ObjectStoreSourceError;
use crate::object_store::Result;

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
        store: &Arc<dyn ObjectStore>,
        _: ResolvedPattern,
    ) -> Result<Vec<ObjectMeta>> {
        let location = ObjectStorePath::default();
        let meta = self.object_meta(store, &location).await?;
        Ok(vec![meta])
    }

    /// Get the object meta from a HEAD request to the url.
    ///
    /// We avoid using object store's `head` method since it does a PROPFIND
    /// request.
    async fn object_meta(
        &self,
        _store: &Arc<dyn ObjectStore>,
        location: &ObjectStorePath,
    ) -> Result<ObjectMeta> {
        let res = reqwest::Client::new().head(self.url.clone()).send().await?;
        let status = res.status();
        if !status.is_success() {
            if self.url.as_str().contains('*') {
                return Err(ObjectStoreSourceError::InvalidHttpStatus(format!(
                    "Unexpected status code '{}' for url: '{}'. Note that globbing is not supported for HTTP.",
                    status, self.url
                )));
            }
            return Err(ObjectStoreSourceError::InvalidHttpStatus(format!(
                "Unexpected status code '{}' for url: '{}'",
                status, self.url
            )));
        }
        // reqwest doesn't check the content length header, instead looks at the contents
        // See: https://github.com/seanmonstar/reqwest/issues/843
        let len: u64 = res
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| res.content_length().unwrap_or(0));
        if len == 0 {
            return Err(ObjectStoreSourceError::Static(
                "Missing content-length header",
            ));
        }

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: len as usize,
            e_tag: None,
            version: None,
        })
    }

    async fn infer_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        state: &SessionState,
        file_format: &dyn FileFormat,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // NOTE: For HTTP, we infer the schema from 1 object and assume all the
        // other objects have the same schema.
        //
        // This is very strange since in many cases the infer schema fails for
        // HTTP, that too randomly. Maybe for some other time to dig into and
        // figure out what's wrong there.

        let object = objects.iter().next().ok_or_else(|| {
            ObjectStoreSourceError::Static(
                "expected at-least 1 object for inferring HTTP object schema",
            )
        })?;

        Ok(file_format
            .infer_schema(state, store, &[object.clone()])
            .await?)
    }
}
