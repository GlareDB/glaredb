use crate::errors::{internal, Result};
use crate::{Blob, StableStorage, Version};
use async_trait::async_trait;
use object_store::{path::Path as ObjectPath, Error as ObjectError, ObjectStore};
use std::sync::Arc;

/// File name used for per-object versioning.
const VERSION_FILE_NAME: &str = "version";

/// An implementation of a stable store backed by object storage.
///
/// Each version of a blob is an individual object within the object store.
#[derive(Debug)]
pub struct ObjectStableStore {
    prefix: String,
    store: Arc<dyn ObjectStore>,
}

impl ObjectStableStore {
    /// Create a new stable store.
    ///
    /// Every object in the store will be prefixed with the provided string. If
    /// attempting to load a previous store, the provided prefix must be same as
    /// the one provided previously.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> ObjectStableStore {
        ObjectStableStore { prefix, store }
    }

    /// Find the latest version for some name.
    async fn version_for_name(&self, name: &str) -> Result<Version> {
        let path = ObjectPath::from(format!("{}/{}/{}", self.prefix, name, VERSION_FILE_NAME));
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(ObjectError::NotFound { .. }) => return Ok(0), // Never before seen blog.
            Err(e) => return Err(e.into()),
        };
        let bs = result.bytes().await?;
        if bs.len() != 8 {
            return Err(internal!(
                "invalid version bytes for named blob: {}, {:?}",
                name,
                bs
            ));
        }
        let version = u64::from_be_bytes(bs[..].try_into().unwrap()); // Length checked above.

        Ok(version)
    }
}

#[async_trait]
impl StableStorage for ObjectStableStore {
    async fn latest<B: Blob>(&self, name: &str) -> Result<B> {
        let version = self.version_for_name(name).await?;
        let path = ObjectPath::from(format!("{}/{}/{}", self.prefix, name, version));
        let bs = self.store.get(&path).await?.bytes().await?;
        let v = serde_json::from_slice(&bs)?;
        Ok(v)
    }

    async fn append<B: Blob>(&self, name: &str, blob: &B) -> Result<Version> {
        let version = self.version_for_name(name).await? + 1;

        // Store new object.
        let path = ObjectPath::from(format!("{}/{}/{}", self.prefix, name, version));
        let buf = serde_json::to_vec(blob)?;
        self.store.put(&path, buf.into()).await?;

        // Update version.
        let path = ObjectPath::from(format!("{}/{}/{}", self.prefix, name, VERSION_FILE_NAME));
        self.store
            .put(&path, u64::to_be_bytes(version).to_vec().into())
            .await?;

        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Blob;
    use object_store_util::temp::TempObjectStore;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct MyBlob {
        pub a: i32,
    }

    impl Blob for MyBlob {}

    #[tokio::test]
    async fn append_many() {
        let store = ObjectStableStore::new(
            Arc::new(TempObjectStore::new().unwrap()),
            "prefix".to_string(),
        );

        // First append...
        let b = MyBlob { a: 1 };
        let mut version = store.append("test", &b).await.unwrap();
        assert_eq!(1, version);

        // Try some more appends.
        for i in 0..3 {
            let b = MyBlob { a: i };
            let new_version = store.append("test", &b).await.unwrap();
            assert!(new_version > version);
            version = new_version;
            let got: MyBlob = store.latest("test").await.unwrap();
            assert_eq!(b, got);
        }
    }
}
