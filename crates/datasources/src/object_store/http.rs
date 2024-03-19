use core::fmt;
use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::FileFormat;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::stream::BoxStream;
use object_store::http::{HttpBuilder, HttpStore};
use object_store::path::Path as ObjectStorePath;
use object_store::{
    ClientConfigKey,
    GetOptions,
    GetResult,
    GetResultPayload,
    ListResult,
    MultipartId,
    ObjectMeta,
    ObjectStore,
    PutOptions,
    PutResult,
};
use tokio::io::AsyncWrite;
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

impl HttpStoreAccess {
    async fn content_length(u: Url) -> Result<Option<u64>> {
        let res = reqwest::Client::new().head(u.clone()).send().await?;
        let status = res.status();
        if !status.is_success() {
            if u.as_str().contains('*') {
                return Err(ObjectStoreSourceError::InvalidHttpStatus(format!(
                    "Unexpected status code '{}' for url: '{}'. \
                    Note that globbing is not supported for HTTP.",
                    status, u,
                )));
            }
            return Err(ObjectStoreSourceError::InvalidHttpStatus(format!(
                "Unexpected status code '{}' for url: '{}'",
                status, u,
            )));
        }
        // reqwest doesn't check the content length header, instead looks at the contents
        // See: https://github.com/seanmonstar/reqwest/issues/843
        let len = res
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| res.content_length());

        Ok(len)
    }
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
            .replace('%', "__percent__")
            .replace('?', "__question__")
            .replace('=', "__equal__");

        Ok(ObjectStoreUrl::parse(u)?)
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let builder = HttpBuilder::new()
            .with_url(self.url.to_string())
            .with_config(ClientConfigKey::AllowHttp, "true");

        let build = builder.build()?;

        Ok(Arc::new(SimpleHttpStore {
            url: self.url.clone(),
            obj_store: build,
        }))
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
        let content_length = Self::content_length(self.url.clone()).await?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: content_length.unwrap_or_default() as usize,
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

#[derive(Debug)]
struct SimpleHttpStore {
    url: Url,
    // Used when content length available.
    obj_store: HttpStore,
}

impl SimpleHttpStore {
    async fn simple_get_req(&self, location: ObjectStorePath) -> Result<GetResult> {
        let res = reqwest::get(self.url.clone()).await?;
        if !res.status().is_success() {
            return Err(ObjectStoreSourceError::InvalidHttpStatus(format!(
                "getting data for '{}' resulted in error status: {}",
                self.url,
                res.status(),
            )));
        }

        // TODO: Maybe write the byte stream to file?
        // Would only be useful when the returned bytes are too many.
        let contents = res.bytes().await?;

        let size = contents.len();

        let stream = async_stream::stream! {
            let res: Result<Bytes, object_store::Error> = Ok(contents);
            yield res;
        };

        let payload = GetResultPayload::Stream(Box::pin(stream));

        Ok(GetResult {
            payload,
            meta: ObjectMeta {
                location,
                last_modified: Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range: 0..size,
        })
    }
}

impl fmt::Display for SimpleHttpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimpleHttpStore({})", self.url)
    }
}

#[async_trait]
impl ObjectStore for SimpleHttpStore {
    async fn put(
        &self,
        location: &ObjectStorePath,
        bytes: Bytes,
    ) -> Result<PutResult, object_store::Error> {
        self.obj_store.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        bytes: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.obj_store.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &ObjectStorePath,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>), object_store::Error> {
        self.obj_store.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &ObjectStorePath,
        multipart_id: &MultipartId,
    ) -> Result<(), object_store::Error> {
        self.abort_multipart(location, multipart_id).await
    }

    // This uses the default impl for `get`, `get_range`, `get_ranges`, `head`
    // (from our custom get_opts impl).

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.range.is_some()
            || options.version.is_some()
            || options.head
        {
            // Let the default implementation handle everything weird.
            self.obj_store.get_opts(location, options).await
        } else {
            // Try to get the content length.
            let content_length = HttpStoreAccess::content_length(self.url.clone())
                .await
                .ok()
                .flatten()
                .unwrap_or_default();

            if content_length != 0 {
                self.obj_store.get_opts(location, options).await
            } else {
                self.simple_get_req(location.clone()).await.map_err(|e| {
                    object_store::Error::Generic {
                        store: "HTTP",
                        source: Box::new(e),
                    }
                })
            }
        }
    }

    async fn delete(&self, location: &ObjectStorePath) -> Result<(), object_store::Error> {
        self.obj_store.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<ObjectStorePath, object_store::Error>>,
    ) -> BoxStream<'a, Result<ObjectStorePath, object_store::Error>> {
        self.obj_store.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
        self.obj_store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectStorePath>,
        offset: &ObjectStorePath,
    ) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
        self.obj_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> Result<ListResult, object_store::Error> {
        self.obj_store.list_with_delimiter(prefix).await
    }

    async fn copy(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> Result<(), object_store::Error> {
        self.obj_store.copy(from, to).await
    }

    async fn rename(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> Result<(), object_store::Error> {
        self.obj_store.rename(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> Result<(), object_store::Error> {
        self.obj_store.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> Result<(), object_store::Error> {
        self.obj_store.rename_if_not_exists(from, to).await
    }
}
