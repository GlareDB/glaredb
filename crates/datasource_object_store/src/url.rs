use crate::{ObjectStoreSourceError, Result};
use futures::stream::{BoxStream, Stream};
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::path::Path;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub enum ObjectStoreAuth {
    S3 {
        access_key_id: String,
        secret_access_key: String,
    },
    Gcs {
        service_account_key: String,
    },
    None,
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectStoreProvider {
    S3,
    Gcs,
    File,
}

impl FromStr for ObjectStoreProvider {
    type Err = ObjectStoreSourceError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "s3" => Self::S3,
            "gcs" => Self::Gcs,
            "file" => Self::File,
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
    provider: ObjectStoreProvider,
}

impl ObjectStoreSourceUrl {
    pub fn parse(loc: &str) -> Result<Self> {
        let url = match url::Url::parse(loc) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                url::Url::parse(&format!("file:///{loc}"))?
            }
            Err(e) => return Err(e.into()),
        };

        let provider = url.scheme().parse()?;

        Ok(ObjectStoreSourceUrl { url, provider })
    }

    pub async fn list_files(
        &self,
        store: &dyn ObjectStore,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let loc = ObjectPath::from(self.url.path());
        let meta = store.head(&loc).await?;
        unimplemented!()
    }

    pub fn get_provider(&self) -> ObjectStoreProvider {
        self.provider
    }

    pub fn bucket_name(&self) -> &str {
        &self.url[url::Position::BeforeHost..url::Position::AfterHost]
    }

    pub fn location(&self) -> &str {
        &self.url[url::Position::BeforePath..url::Position::AfterPath]
    }

    pub fn extension(&self) -> Option<&str> {
        let path = self.location();
        Path::new(path).extension().and_then(|ext| ext.to_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let u = ObjectStoreSourceUrl::parse("gs://my_bucket/my_obj").unwrap();
        assert_eq!("my_bucket", u.bucket_name());
        assert_eq!("/my_obj", u.location());
        assert_eq!(None, u.extension());

        let u = ObjectStoreSourceUrl::parse("gs://my_bucket/my_obj.parquet").unwrap();
        assert_eq!("my_bucket", u.bucket_name());
        assert_eq!("/my_obj.parquet", u.location());
        assert_eq!(Some("parquet"), u.extension());

        let u = ObjectStoreSourceUrl::parse("./my_bucket/my_obj.parquet").unwrap();
        assert_eq!("", u.bucket_name());
        assert_eq!("/my_bucket/my_obj.parquet", u.location());
        assert_eq!(Some("parquet"), u.extension());

        let u = ObjectStoreSourceUrl::parse("/Users/mario/my_bucket/my_obj").unwrap();
        assert_eq!("", u.bucket_name());
        assert_eq!("/Users/mario/my_bucket/my_obj", u.location());
        assert_eq!(None, u.extension());
    }
}
