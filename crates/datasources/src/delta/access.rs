use crate::delta::catalog::{DataCatalog, UnityCatalog};
use crate::delta::errors::Result;
use deltalake::DeltaTable;
use metastoreproto::types::options::{DeltaLakeCatalog, DeltaLakeUnityCatalog};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Access a delta lake.
pub struct DeltaLakeAccessor {
    catalog: Arc<dyn DataCatalog>,
    region: String,
    access_key_id: String,
    secret_access_key: String,
}

impl DeltaLakeAccessor {
    /// Connect to a deltalake using the provided catalog information.
    // TODO: Allow accessing delta tables without a catalog?
    // TODO: Don't be S3 specific.
    pub async fn connect(
        catalog: &DeltaLakeCatalog,
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
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
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
        })
    }

    pub async fn load_table(self, database: &str, table: &str) -> Result<DeltaTable> {
        let loc = self
            .catalog
            .get_table_storage_location(database, table)
            .await?;

        debug!(%loc, %database, %table, "deltalake location");

        let mut opts = HashMap::new();
        opts.insert("aws_access_key_id".to_string(), self.access_key_id);
        opts.insert("aws_secret_access_key".to_string(), self.secret_access_key);
        opts.insert("aws_region".to_string(), self.region);

        let table = deltalake::open_table_with_storage_options(loc, opts).await?;

        // Note that the deltalake crate does the appropriate jank for
        // registering the object store in the datafusion session's runtime env
        // during execution.
        Ok(table)
    }
}
