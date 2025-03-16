use std::fmt;
use std::path::{Path, PathBuf};

use glaredb_error::{OptionExt, RayexecError, Result, ResultExt};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::s3::credentials::AwsCredentials;

/// Configuration for accessing various object stores.
///
/// The variant used determines how we should interpret the file location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessConfig {
    S3 {
        credentials: AwsCredentials,
        region: String,
    },
    None,
}

impl ProtoConv for AccessConfig {
    type ProtoType = rayexec_proto::generated::access::AccessConfig;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::access::access_config::Value;
        use rayexec_proto::generated::access::{AwsCredentials, EmptyAccessConfig, S3AccessConfig};

        let value = match self {
            Self::S3 {
                credentials,
                region,
            } => Value::S3(S3AccessConfig {
                credentials: Some(AwsCredentials {
                    key_id: credentials.key_id.clone(),
                    secret: credentials.secret.clone(),
                }),
                region: region.clone(),
            }),
            Self::None => Value::None(EmptyAccessConfig {}),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::access::access_config::Value;

        Ok(match proto.value.required("value")? {
            Value::None(_) => Self::None,
            Value::S3(s3) => {
                let credentials = s3.credentials.required("credentials")?;
                Self::S3 {
                    credentials: AwsCredentials {
                        key_id: credentials.key_id,
                        secret: credentials.secret,
                    },
                    region: s3.region,
                }
            }
        })
    }
}

/// Location for a file.
// TODO: Glob/hive
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileLocation {
    Url(Url),
    Path(PathBuf),
}

impl FileLocation {
    /// Parse a file location from a string.
    ///
    /// Current implementation assumes that if the string fails to parse as a
    /// url, it must be a path. However further checks will need to be done when
    /// we support globs and hive partitioning here.
    pub fn parse(s: &str) -> Self {
        match Url::parse(s) {
            Ok(url) => FileLocation::Url(url),
            Err(_) => FileLocation::Path(PathBuf::from(s)),
        }
    }

    pub const fn is_path(&self) -> bool {
        matches!(self, FileLocation::Path(_))
    }

    pub const fn is_url(&self) -> bool {
        matches!(self, FileLocation::Url(_))
    }

    /// Return a new location with additional paths segments added.
    pub fn join<S: AsRef<str>>(&self, segments: impl IntoIterator<Item = S>) -> Result<Self> {
        let mut loc = self.clone();
        loc.join_mut(segments)?;
        Ok(loc)
    }

    /// Mutably joins additional path segments to the file location.
    pub fn join_mut<S: AsRef<str>>(&mut self, segments: impl IntoIterator<Item = S>) -> Result<()> {
        match self {
            Self::Url(url) => {
                url.path_segments_mut()
                    .map_err(|_| RayexecError::new("Failed to get path segments for url"))?
                    .extend(segments);
            }
            Self::Path(path) => {
                for seg in segments {
                    path.push(Path::new(seg.as_ref()));
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for FileLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(u) => write!(f, "{}", u),
            Self::Path(p) => write!(f, "{}", p.display()),
        }
    }
}

impl ProtoConv for FileLocation {
    type ProtoType = rayexec_proto::generated::access::FileLocation;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::access::file_location::Value;

        let value = match self {
            Self::Url(url) => Value::Url(url.to_string()),
            Self::Path(path) => Value::Path(
                path.to_str()
                    .ok_or_else(|| RayexecError::new("path not utf8"))?
                    .to_string(),
            ),
        };
        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::access::file_location::Value;

        Ok(match proto.value.required("value")? {
            Value::Url(url) => Self::Url(Url::parse(&url).context("failed to parse url")?),
            Value::Path(path) => Self::Path(PathBuf::from(path)),
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_proto::testutil::assert_proto_roundtrip;

    use super::*;

    #[test]
    fn location_join_path() {
        let mut location = FileLocation::parse("./dir/");
        assert!(location.is_path());
        location.join_mut(["a", "b"]).unwrap();
        assert_eq!("./dir/a/b", location.to_string());

        location.join_mut(["c"]).unwrap();
        assert_eq!("./dir/a/b/c", location.to_string());

        location.join_mut(["d/e"]).unwrap();
        assert_eq!("./dir/a/b/c/d/e", location.to_string());
    }

    #[test]
    fn location_join_url() {
        let mut location = FileLocation::parse("s3://bucket/path");
        assert!(location.is_url());
        location.join_mut(["a", "b"]).unwrap();
        assert_eq!("s3://bucket/path/a/b", location.to_string());

        location.join_mut(["c"]).unwrap();
        assert_eq!("s3://bucket/path/a/b/c", location.to_string());

        // TODO: Should this be allowed?
        // location.join_mut(["d/e"]).unwrap();
        // assert_eq!("s3://bucket/path/a/b/c/d/e", location.to_string());
    }

    #[test]
    fn location_proto_roundtrip() {
        let location = FileLocation::parse("./some/file.parquet");
        assert_proto_roundtrip(location);

        let location = FileLocation::parse("https://example.com/file.json");
        assert_proto_roundtrip(location);

        let location = FileLocation::parse("s3://bucket/to/file.csv");
        assert_proto_roundtrip(location);
    }
}
