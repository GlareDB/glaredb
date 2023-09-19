use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use datafusion::{
    arrow::datatypes::Schema,
    datasource::{file_format::FileFormat, TableProvider},
    error::DataFusionError,
    execution::{context::SessionState, object_store::ObjectStoreUrl},
};
use object_store::{http::HttpBuilder, path::Path as ObjectStorePath, ObjectMeta, ObjectStore};
use url::Url;

use crate::{
    common::url::DatasourceUrl,
    object_store::{errors::ObjectStoreSourceError, Result},
};

use super::{MultiSourceTableProvider, ObjStoreAccess, ObjStoreTableProvider};

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
        _store: &Arc<dyn ObjectStore>,
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

    async fn create_table_provider(
        &self,
        state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        locations: Vec<DatasourceUrl>,
    ) -> Result<Arc<dyn TableProvider>> {
        let store = self.create_store()?;
        let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();

        let mut locations = locations.into_iter();

        let next = locations
            .next()
            .ok_or(ObjectStoreSourceError::Static("No locations provided"))?;

        let objects = self
            .list_globbed(&store, &next.path())
            .await
            .map_err(|_| DataFusionError::Plan("unable to list globbed".to_string()))?;

        // this assumes that all locations have the same schema.
        let arrow_schema = file_format
            .clone()
            .infer_schema(state, &store, &objects)
            .await?;

        let base_url = self.base_url()?;

        let prov = Arc::new(ObjStoreTableProvider {
            store: store.clone(),
            arrow_schema: arrow_schema.clone(),
            file_format: file_format.clone(),
            base_url,
            objects,
            _predicate_pushdown: true,
        });
        providers.push(prov);

        for loc in locations {
            let store = store.clone();
            let arrow_schema = arrow_schema.clone();
            let file_format = file_format.clone();
            let prov = self
                .create_table_provider_single(loc, store, arrow_schema, file_format)
                .await?;

            providers.push(Arc::new(prov));
        }
        Ok(Arc::new(MultiSourceTableProvider::new(providers)))
    }
}

impl HttpStoreAccess {
    async fn create_table_provider_single(
        &self,
        url: DatasourceUrl,
        store: Arc<dyn ObjectStore>,
        arrow_schema: Arc<Schema>,
        file_format: Arc<dyn FileFormat>,
    ) -> Result<ObjStoreTableProvider> {
        let base_url = self.base_url()?;
        let objects = self
            .list_globbed(&store, &url.path())
            .await
            .map_err(|_| DataFusionError::Plan("unable to list globbed".to_string()))?;

        Ok(ObjStoreTableProvider {
            store,
            arrow_schema,
            base_url,
            objects,
            file_format,
            _predicate_pushdown: true,
        })
    }
}
