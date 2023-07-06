use std::sync::Arc;

use datafusion::datasource::TableProvider;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::csv::CsvTableProvider;
use super::errors::Result;
use super::json::JsonTableProvider;
use super::parquet::ParquetTableProvider;
use super::{file_type_from_path, FileType, TableAccessor};

/// Information needed for accessing an Parquet file on local file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    pub location: String,
    pub file_type: Option<FileType>,
}

impl LocalTableAccess {
    pub fn store_and_path(&self) -> Result<(Arc<dyn ObjectStore>, ObjectStorePath)> {
        let store = Arc::new(LocalFileSystem::new());
        let location = ObjectStorePath::from_filesystem_path(self.location.as_str())?;
        Ok((store, location))
    }
}

#[derive(Debug)]
pub struct LocalAccessor {
    /// Local filesystem  object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
}

#[async_trait::async_trait]
impl TableAccessor for LocalAccessor {
    fn location(&self) -> String {
        self.meta.location.to_string()
    }
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }

    async fn into_table_provider(self, predicate_pushdown: bool) -> Result<Arc<dyn TableProvider>> {
        let table_provider: Arc<dyn TableProvider> = match self.file_type {
            FileType::Parquet => {
                Arc::new(ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await?)
            }
            FileType::Csv => Arc::new(CsvTableProvider::from_table_accessor(self).await?),
            FileType::Json => Arc::new(JsonTableProvider::from_table_accessor(self).await?),
        };
        Ok(table_provider)
    }
}

impl LocalAccessor {
    /// Setup accessor for Local file system
    pub async fn new(access: LocalTableAccess) -> Result<Self> {
        let (store, location) = access.store_and_path()?;
        // Use provided file type or infer from location
        let file_type = access.file_type.unwrap_or(file_type_from_path(&location)?);
        trace!(?location, ?file_type, "location and file type");

        let meta = Arc::new(store.head(&location).await?);

        Ok(Self {
            store,
            meta,
            file_type,
        })
    }

    pub async fn validate_table_access(access: LocalTableAccess) -> Result<()> {
        let store = Arc::new(LocalFileSystem::new());

        let location = ObjectStorePath::from_filesystem_path(access.location)?;
        store.head(&location).await?;
        Ok(())
    }
}
