use std::sync::Arc;

use crate::object_store::{errors::ObjectStoreSourceError, Result, TableAccessor};

use chrono::{DateTime, Utc};
use datafusion::datasource::TableProvider;
use object_store::{ObjectMeta, ObjectStore};

use super::{
    csv::CsvTableProvider, json::JsonTableProvider, parquet::ParquetTableProvider, FileType,
};

#[derive(Debug, Clone)]
pub struct HttpTableAccess {
    /// Public url where the file is hosted.
    pub url: String,
}

#[derive(Debug)]
pub struct HttpAccessor {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
}

impl HttpAccessor {
    pub async fn try_new(url: String, file_type: FileType) -> Result<Self> {
        let meta = object_meta_from_head(&url).await?;
        let builder = object_store::http::HttpBuilder::new();
        let store = builder.with_url(url).build()?;
        Ok(Self {
            store: Arc::new(store),
            meta: Arc::new(meta),
            file_type,
        })
    }
}

#[async_trait::async_trait]
impl TableAccessor for HttpAccessor {
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
async fn object_meta_from_head(url: &str) -> Result<ObjectMeta> {
    use object_store::path::Path as ObjectStorePath;
    let url = url::Url::parse(url).unwrap();

    let res = reqwest::Client::new().head(url).send().await?;

    let len = res.content_length().ok_or(ObjectStoreSourceError::Static(
        "Missing content-length header",
    ))?;

    Ok(ObjectMeta {
        location: ObjectStorePath::default(),
        last_modified: DateTime::<Utc>::MIN_UTC,
        size: len as usize,
    })
}
