use crate::delta::catalog::{DataCatalog, UnityCatalog};
use crate::delta::errors::{DeltaError, Result};
use deltalake::storage::DeltaObjectStore;
use deltalake::DeltaTable;
use metastoreproto::types::options::{DeltaLakeCatalog, DeltaLakeUnityCatalog};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use std::sync::Arc;

/// Access a delta lake.
pub struct DeltaLakeAccessor {
    catalog: Arc<dyn DataCatalog>,
    store: Arc<dyn ObjectStore>,
}

impl DeltaLakeAccessor {
    /// Connect to a deltalake using the provided catalog information.
    // TODO: Allow accessing delta tables without a catalog?
    // TODO: Don't be S3 specific.
    pub async fn connect(
        catalog: &DeltaLakeCatalog,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<DeltaLakeAccessor> {
        let store = Arc::new(
            AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .build()?,
        );

        let catalog: Arc<dyn DataCatalog> = match catalog {
            DeltaLakeCatalog::Unity(DeltaLakeUnityCatalog {
                catalog_id,
                databricks_access_token,
                workspace_url,
            }) => {
                let catalog =
                    UnityCatalog::connect(databricks_access_token, workspace_url, catalog_id)
                        .await?;
                Arc::new(catalog)
            }
        };

        Ok(DeltaLakeAccessor { catalog, store })
    }

    pub async fn load_table(self, database: &str, table: &str) -> Result<DeltaTable> {
        let loc = self
            .catalog
            .get_table_storage_location(database, table)
            .await?;

        let loc = url::Url::parse(&loc)?;
        let store = Arc::new(DeltaObjectStore::new(self.store, loc));

        let mut table = DeltaTable::new(
            store,
            deltalake::DeltaTableConfig {
                require_tombstones: false,
                require_files: false,
            },
        );

        table.load().await?;

        // Note that the deltalake crate does the appropriate jank for
        // registering the object store in the datafusion session's runtime env
        // during execution.
        Ok(table)
    }
}
