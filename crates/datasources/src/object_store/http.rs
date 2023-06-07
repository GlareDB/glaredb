use super::csv::CsvTableProvider;
use super::errors::{ObjectStoreSourceError, Result};
use super::parquet::ParquetTableProvider;
use super::{file_type_from_path, FileType, TableAccessor};
use chrono::{DateTime, Utc};
use datafusion::datasource::TableProvider;
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;

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

impl TableAccessor for HttpAccessor {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }
}

impl HttpAccessor {
    pub async fn new(access: HttpTableAccess) -> Result<Self> {
        let meta = Arc::new(object_meta_from_head(&access.url).await?);

        let location = ObjectStorePath::from(access.url.clone());
        let file_type = file_type_from_path(&location)?;

        let store = Arc::new(HttpBuilder::new().with_url(access.url).build()?);

        Ok(Self {
            store,
            meta,
            file_type,
        })
    }

    pub async fn validate_table_access(access: HttpTableAccess) -> Result<()> {
        object_meta_from_head(&access.url).await?;
        Ok(())
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_provider: Arc<dyn TableProvider> = match self.file_type {
            FileType::Parquet => {
                Arc::new(ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await?)
            }
            FileType::Csv => Arc::new(CsvTableProvider::from_table_accessor(self).await?),
        };
        Ok(table_provider)
    }
}

/// Get the object meta from a HEAD request to the url.
///
/// We avoid using object store's `head` method since it does a PROPFIND
/// request.
async fn object_meta_from_head(url: &str) -> Result<ObjectMeta> {
    let url = url::Url::parse(url)?;

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
