use crate::csv::CsvTableProvider;
use crate::errors::Result;
use crate::listing::{build_url_with_hash, ObjectStoreHasher};
use crate::parquet::ParquetTableProvider;
use crate::{file_type_from_path, FileType, TableAccessor};
use datafusion::datasource::TableProvider;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::trace;

/// Information needed for accessing an Parquet file on local file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    pub location: String,
    pub file_type: Option<FileType>,
}

impl ObjectStoreHasher for LocalTableAccess {
    fn generate_url(&self) -> url::Url {
        build_url_with_hash("local", "local", "local")
    }

    fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        Ok(Arc::new(LocalFileSystem::new()))
    }
}

#[derive(Debug)]
pub struct LocalAccessor {
    pub access: LocalTableAccess,
    pub file_type: FileType,
}

impl LocalAccessor {
    /// Setup accessor for Local file system
    pub async fn new(access: LocalTableAccess) -> Result<Self> {
        unimplemented!()
        // Ok(Self { access, file_type })
    }

    pub async fn validate_table_access(access: LocalTableAccess) -> Result<()> {
        let store = Arc::new(LocalFileSystem::new());
        let location = ObjectStorePath::from_filesystem_path(access.location)?;
        store.head(&location).await?;
        Ok(())
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        unimplemented!()
        // let table_provider: Arc<dyn TableProvider> = match self.file_type {
        //     FileType::Parquet => {
        //         Arc::new(ParquetTableProvider::from_table_accessor(self, predicate_pushdown).await?)
        //     }
        //     FileType::Csv => Arc::new(CsvTableProvider::from_table_accessor(self).await?),
        // };
        // Ok(table_provider)
    }
}
