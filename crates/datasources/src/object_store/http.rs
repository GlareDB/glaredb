use std::{sync::Arc, fmt::Display};

use crate::object_store::{errors::ObjectStoreSourceError, Result};

use async_trait::async_trait;
use chrono::Utc;
use datafusion::{
    datasource::{listing::ListingTableConfig, TableProvider},
    execution::context::SessionState,
};
use object_store::{path::Path as ObjectStorePath, ObjectMeta, ObjectStore, http::HttpBuilder};
use url::Url;

use std::any::Any;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result as DatafusionResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::Expr;

pub struct HttpProvider {
    pub base_url: String,
    pub config: ListingTableConfig,
    pub paths: Vec<ObjectMeta>,
}

#[async_trait]
impl TableProvider for HttpProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.config.clone().file_schema.unwrap()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let url = url::Url::parse(&self.base_url).unwrap();
        let base_url = format!("{}://{}", url.scheme(), url.authority());

        if self.config.table_paths.len() > 1 {
            let mut execs = Vec::with_capacity(self.config.table_paths.len());
            for p in self.config.table_paths.clone() {
                let p = p.to_string();
                let url = url::Url::parse(&p).unwrap();
                let path = url.path();

                let path = PartitionedFile::new(path.to_string(), 0);
                let object_store_url = ObjectStoreUrl::parse(base_url.clone())
                    .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem());
                let conf = FileScanConfig {
                    object_store_url,
                    file_schema: self.schema(),
                    file_groups: vec![vec![path]],
                    statistics: Statistics::default(),
                    projection: projection.cloned(),
                    limit,
                    table_partition_cols: Vec::new(),
                    output_ordering: Vec::new(),
                    infinite_source: false,
                };
                let exec = self
                    .config
                    .clone()
                    .options
                    .unwrap()
                    .format
                    .create_physical_plan(ctx, conf, None)
                    .await?;
                execs.push(exec);
            }
            Ok(Arc::new(UnionExec::new(execs)))
        } else {
            let path = self.config.table_paths.get(0).unwrap();
            let p = path.to_string();
            let url = url::Url::parse(&p).unwrap();
            let path = url.path();

            let path = PartitionedFile::new(path.to_string(), 0);

            let object_store_url = ObjectStoreUrl::parse(base_url)
                .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem());
            let conf = FileScanConfig {
                object_store_url,
                file_schema: self.schema(),
                file_groups: vec![vec![path]],
                statistics: Statistics::default(),
                projection: projection.cloned(),
                limit,
                table_partition_cols: Vec::new(),
                output_ordering: Vec::new(),
                infinite_source: false,
            };

            self.config
                .clone()
                .options
                .unwrap()
                .format
                .create_physical_plan(ctx, conf, None)
                .await
        }
    }
}

/// Get the object meta from a HEAD request to the url.
///
/// We avoid using object store's `head` method since it does a PROPFIND
/// request.
pub async fn object_meta_from_head(url: &url::Url) -> Result<ObjectMeta> {
    let res = reqwest::Client::new().head(url.clone()).send().await?;

    let len = res.content_length().ok_or(ObjectStoreSourceError::Static(
        "Missing content-length header",
    ))?;
    Ok(ObjectMeta {
        // Note that we're not providing a path here since the http object store
        // will already have the full url to use.
        //
        // This is a workaround for object store percent encoding already
        // percent encoded paths.
        location: ObjectStorePath::default(),
        last_modified: Utc::now(),
        size: len as usize,
        e_tag: None,
    })
}


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
