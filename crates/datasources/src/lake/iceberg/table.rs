use crate::lake::iceberg::errors::{IcebergError, Result};
use crate::lake::iceberg::spec::TableMetadata;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;

#[derive(Debug)]
pub struct IcebergTable {
    /// The root of the table.
    location: String,

    /// Store for accessing the table.
    store: Arc<dyn ObjectStore>,

    metadata: TableMetadata,
}

impl IcebergTable {
    /// Open a table at a location using the provided object store.
    pub async fn open(location: String, store: Arc<dyn ObjectStore>) -> Result<IcebergTable> {
        // Get version.
        let version = {
            let path = ObjectPath::from_iter([&location, "metadata/version-hint.text"]);
            let bs = store.get(&path).await?.bytes().await?;
            let s = String::from_utf8(bs.to_vec()).map_err(|e| {
                IcebergError::DataInvalid(format!("Expected utf-8 in version hint: {}", e))
            })?;

            let v = s.parse::<i32>().map_err(|e| {
                IcebergError::DataInvalid(format!("Version hint to be a number: {}", e))
            })?;

            // Only support v2 right now.
            if v != 2 {
                return Err(IcebergError::DataInvalid(format!(
                    "Only format version v2 is supported"
                )));
            }

            v
        };

        // Read metadata.
        let metadata = {
            let path =
                ObjectPath::from_iter([location.clone(), format!("v{version}.metadata.json")]);
            let bs = store.get(&path).await?.bytes().await?;
            let metadata: TableMetadata = serde_json::from_slice(&bs).map_err(|e| {
                IcebergError::DataInvalid(format!("Failed to read table metadata: {}", e))
            })?;
            metadata
        };

        Ok(IcebergTable {
            location,
            store,
            metadata,
        })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}
