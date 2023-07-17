use std::sync::Arc;

use crate::object_store::{errors::ObjectStoreSourceError, Result, TableAccessor};

use chrono::Utc;
use datafusion::datasource::TableProvider;
use object_store::{path::Path as ObjectStorePath, ObjectMeta, ObjectStore};

use super::{
    csv::CsvTableProvider, json::JsonTableProvider, parquet::ParquetTableProvider, FileType,
};

#[derive(Debug)]
pub struct HttpAccessor {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
    base_url: String,
}

impl HttpAccessor {
    pub async fn try_new(url: String, file_type: FileType) -> Result<Self> {
        let url = url::Url::parse(&url).unwrap();
        let meta = object_meta_from_head(&url).await?;
        let builder = object_store::http::HttpBuilder::new();
        let base_url = format!("{}://{}", url.scheme(), url.authority());
        let store = builder.with_url(url.to_string()).build()?;

        Ok(Self {
            store: Arc::new(store),
            meta: Arc::new(meta),
            file_type,
            base_url,
        })
    }
}

#[async_trait::async_trait]
impl TableAccessor for HttpAccessor {
    fn base_path(&self) -> String {
        self.base_url.clone()
    }

    fn location(&self) -> String {
        self.meta.location.to_string()
    }

    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }

    async fn into_table_provider(self, _: bool) -> Result<Arc<dyn TableProvider>> {
        let table_provider: Arc<dyn TableProvider> = match self.file_type {
            FileType::Parquet => {
                Arc::new(ParquetTableProvider::from_table_accessor(self, true).await?)
            }
            FileType::Csv => Arc::new(CsvTableProvider::from_table_accessor(self).await?),
            FileType::Json => Arc::new(JsonTableProvider::from_table_accessor(self).await?),
        };

        Ok(table_provider)
    }
}

/// Get the object meta from a HEAD request to the url.
///
/// We avoid using object store's `head` method since it does a PROPFIND
/// request.
async fn object_meta_from_head(url: &url::Url) -> Result<ObjectMeta> {
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
