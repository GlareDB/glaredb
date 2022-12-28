use crate::errors::{internal, Result, StableStorageError};
use crate::{Blob, StableStorage, Version, VersionReadOption};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{path::Path as ObjectPath, Error as ObjectError, ObjectStore};
use std::sync::Arc;
use tracing::{info, info_span, Instrument};
use uuid::Uuid;

/// File name used for per-object versioning.
const VERSION_FILE_NAME: &str = "version";

/// An implementation of a stable store backed by object storage.
///
/// Each version of a blob is an individual object within the object store.
#[derive(Debug)]
pub struct ObjectStableStore {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStableStore {
    /// Open a stable store backed by object storage.
    ///
    /// Every object in the store will be prefixed with the provided string. If
    /// attempting to load a previous store, the provided prefix must be same as
    /// the one provided previously.
    pub async fn open(store: Arc<dyn ObjectStore>) -> Result<ObjectStableStore> {
        // Ensure we have permissions by writing to and reading from a random
        // file.
        {
            let store = store.clone();
            let id = Uuid::new_v4().to_string();
            let loc = ObjectPath::from(format!("perm_check-{}", id));

            let span = info_span!("object_store_permissions_check", %store, %loc);
            async move {
                let bytes = Bytes::from("Hello world!");
                let range = 0..bytes.len();

                info!("testing put");
                store.put(&loc, bytes.clone()).await?;

                info!("testing get_range");
                let got = store.get_range(&loc, range).await?;

                if got != bytes {
                    return Err(internal!(
                        "bytes mismtach from object storage; got: {:?}, expected: {:?}",
                        got,
                        bytes
                    ));
                }

                info!("testing delete");
                store.delete(&loc).await?;

                info!("able to put, get_range, and delete");

                Ok::<(), StableStorageError>(())
            }
            .instrument(span)
            .await?;
        }

        Ok(ObjectStableStore { store })
    }

    /// Find the latest version for some name.
    async fn version_for_name(&self, name: &str) -> Result<Version> {
        let path = ObjectPath::from(format!("{}/{}", name, VERSION_FILE_NAME));
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(ObjectError::NotFound { .. }) => return Ok(0), // Never before seen blob.
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
    async fn read<B: Blob>(&self, name: &str, opt: VersionReadOption) -> Result<Option<B>> {
        let version = match opt {
            VersionReadOption::Latest => self.version_for_name(name).await?,
            VersionReadOption::Version(v) => v,
        };
        let path = ObjectPath::from(format!("{}/{}", name, version));
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(ObjectError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let bs = result.bytes().await?;
        let v = serde_json::from_slice(&bs)?;
        Ok(v)
    }

    async fn append<B: Blob>(&self, name: &str, blob: &B) -> Result<Version> {
        let version = self.version_for_name(name).await? + 1;

        // Store new object.
        let path = ObjectPath::from(format!("{}/{}", name, version));
        let buf = serde_json::to_vec(blob)?;
        self.store.put(&path, buf.into()).await?;

        // Update version.
        let path = ObjectPath::from(format!("{}/{}", name, VERSION_FILE_NAME));
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
        let store = ObjectStableStore::open(Arc::new(TempObjectStore::new().unwrap()))
            .await
            .unwrap();

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
            let got: Option<MyBlob> = store.read("test", VersionReadOption::Latest).await.unwrap();
            assert_eq!(Some(b), got);
        }
    }

    #[tokio::test]
    async fn read_version() {
        let store = ObjectStableStore::open(Arc::new(TempObjectStore::new().unwrap()))
            .await
            .unwrap();

        let a = MyBlob { a: 1 };
        let first = store.append("test", &a).await.unwrap();

        let b = MyBlob { a: 2 };
        let second = store.append("test", &b).await.unwrap();

        assert_ne!(first, second);

        // Read first version.
        let got: Option<MyBlob> = store
            .read("test", VersionReadOption::Version(first))
            .await
            .unwrap();
        assert_eq!(Some(a), got);

        // Read second version.
        let got: Option<MyBlob> = store
            .read("test", VersionReadOption::Version(second))
            .await
            .unwrap();
        assert_eq!(Some(b), got);
    }
}
