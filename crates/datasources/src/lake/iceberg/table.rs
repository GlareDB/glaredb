use crate::common::url::DatasourceUrl;
use crate::lake::iceberg::errors::{IcebergError, Result};
use crate::lake::iceberg::spec::TableMetadata;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;

#[derive(Debug)]
pub struct IcebergTable {
    /// The root of the table.
    location: DatasourceUrl,

    /// Store for accessing the table.
    store: Arc<dyn ObjectStore>,

    metadata: TableMetadata,
}

impl IcebergTable {
    /// Open a table at a location using the provided object store.
    pub async fn open(
        location: DatasourceUrl,
        store: Arc<dyn ObjectStore>,
    ) -> Result<IcebergTable> {
        // Get version.
        let version = {
            let path = format_object_path(&location, "metadata/version-hint.text")?;
            let path = ObjectPath::parse(path)?;
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
            let path = format_object_path(&location, format!("metadata/v{version}.metadata.json"))?;
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

/// Formats an object path depending on if it's a url (for real object stores),
/// or if it's a local path.
fn format_object_path(
    url: &DatasourceUrl,
    path: impl AsRef<str>,
) -> Result<ObjectPath, object_store::path::Error> {
    let path = path.as_ref();
    match url {
        DatasourceUrl::Url(_) => {
            let path = format!("{}/{path}", url.path());
            ObjectPath::parse(path)
        }
        DatasourceUrl::File(root_path) => {
            let path = root_path.join(path);
            ObjectPath::from_filesystem_path(path)
        }
    }
}
