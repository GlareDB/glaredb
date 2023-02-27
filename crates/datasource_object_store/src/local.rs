use std::sync::Arc;

use datafusion::datasource::TableProvider;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::csv::CsvTableProvider;
use crate::errors::Result;
use crate::parquet::ParquetTableProvider;
use crate::{file_type_from_path, FileType, TableAccessor};

/// Information needed for accessing an Parquet file on local file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    pub location: String,
    pub file_type: Option<FileType>,
}

#[derive(Debug)]
pub struct LocalAccessor {
    /// Local filesystem  object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
    pub file_type: FileType,
}

impl TableAccessor for LocalAccessor {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn object_meta(&self) -> &Arc<ObjectMeta> {
        &self.meta
    }
}

impl LocalAccessor {
    /// Setup accessor for Local file system
    pub async fn new(access: LocalTableAccess) -> Result<Self> {
        let store = Arc::new(LocalFileSystem::new());

        let location = ObjectStorePath::from(access.location);
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

        let location = ObjectStorePath::from(access.location);
        store.head(&location).await?;
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
