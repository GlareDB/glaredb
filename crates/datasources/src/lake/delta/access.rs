use std::collections::HashMap;
use std::sync::Arc;

use deltalake::DeltaTable;
use protogen::metastore::types::options::{
    DeltaLakeCatalog,
    DeltaLakeUnityCatalog,
    StorageOptions,
};
use tracing::debug;

use crate::lake::delta::catalog::{DataCatalog, UnityCatalog};
use crate::lake::delta::errors::Result;

/// Access a delta lake using a catalog.
pub struct DeltaLakeAccessor {
    catalog: Arc<dyn DataCatalog>,
    storage_options: StorageOptions,
}

impl DeltaLakeAccessor {
    /// Connect to a deltalake using the provided catalog information.
    // TODO: Allow accessing delta tables without a catalog?
    pub async fn connect(
        catalog: &DeltaLakeCatalog,
        storage_options: StorageOptions,
    ) -> Result<DeltaLakeAccessor> {
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

        Ok(DeltaLakeAccessor {
            catalog,
            storage_options,
        })
    }

    pub async fn load_table(self, database: &str, table: &str) -> Result<DeltaTable> {
        let loc = self
            .catalog
            .get_table_storage_location(database, table)
            .await?;

        debug!(%loc, %database, %table, "deltalake location");

        let table = load_table_direct(&loc, self.storage_options).await?;
        Ok(table)
    }
}

/// Loads the table at the given location.
pub async fn load_table_direct(location: &str, opts: StorageOptions) -> Result<DeltaTable> {
    // Convert to delta-rs compatible options
    let opts = HashMap::from_iter(opts.inner.into_iter());
    let table = deltalake::open_table_with_storage_options(location, opts).await?;

    // Note that the deltalake crate does the appropriate jank for
    // registering the object store in the datafusion session's runtime env
    // during execution.
    Ok(table)
}
