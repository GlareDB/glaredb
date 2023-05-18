use crate::{ObjectStoreSourceError, Result};
use futures::stream::{BoxStream, Stream};
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum ObjectStoreProvider {
    S3,
    GCS,
}

impl FromStr for ObjectStoreProvider {
    type Err = ObjectStoreSourceError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "s3" => Self::S3,
            "gcs" => Self::GCS,
            other => {
                return Err(ObjectStoreSourceError::UnknownObjectStoreProvider(
                    other.to_string(),
                ))
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreSourceUrl {
    url: url::Url,
}

impl ObjectStoreSourceUrl {
    pub fn parse(loc: &str) -> Result<Self> {
        let url = url::Url::parse(loc)?;
        Ok(ObjectStoreSourceUrl { url })
    }

    pub async fn list_files(
        &self,
        store: &dyn ObjectStore,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let loc = ObjectPath::from(self.url.path());
        let meta = store.head(&loc).await?;
        unimplemented!()
    }
}
