use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::errors::Result;
use crate::parquet::ParquetTableProvider;
use crate::TableAccessor;

/// Information needed for accessing an Parquet file on local file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    pub location: String,
}

#[derive(Debug)]
pub struct LocalAccessor {
    /// Local filesystem  object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
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
        trace!(?location, "location");
        let meta = Arc::new(store.head(&location).await?);

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(
        self,
        predicate_pushdown: bool,
    ) -> Result<ParquetTableProvider<LocalAccessor>> {
        crate::parquet::into_table_provider(self, predicate_pushdown).await
    }
}
