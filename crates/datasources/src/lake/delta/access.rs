use crate::lake::delta::catalog::{DataCatalog, UnityCatalog};
use crate::lake::delta::errors::Result;
use crate::lake::LakeStorageOptions;
use deltalake::DeltaTable;
use protogen::metastore::types::options::{
    CredentialsOptionsAws, DeltaLakeCatalog, DeltaLakeUnityCatalog,
};
use std::sync::Arc;
use tracing::debug;

/// Access a delta lake using a catalog.
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

        // Currently we only support delta lake on S3.
        let opts = LakeStorageOptions::S3 {
            creds: CredentialsOptionsAws {
                access_key_id: self.access_key_id,
                secret_access_key: self.secret_access_key,
            },
            region: self.region,
        };

        let table = load_table_direct(&loc, opts).await?;

        Ok(table)
    }
}

/// Loads the table at the given location.
pub async fn load_table_direct(location: &str, opts: LakeStorageOptions) -> Result<DeltaTable> {
    let opts = opts.into_opts_hashmap();
    let table = deltalake::open_table_with_storage_options(location, opts).await?;

    // Note that the deltalake crate does the appropriate jank for
    // registering the object store in the datafusion session's runtime env
    // during execution.
    Ok(table)
}
