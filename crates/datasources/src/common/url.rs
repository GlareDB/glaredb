//! Utility for source "URLs".

use std::{borrow::Cow, fmt::Display, path::PathBuf};

use url::Url;

use super::errors::{DatasourceCommonError, Result};

#[derive(Debug, Clone)]
pub enum DatasourceUrl {
    File(PathBuf),
    Url(Url),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasourceUrlScheme {
    File,
    Http,
    Gcs,
    S3,
}

impl Display for DatasourceUrlScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File => write!(f, "file"),
            Self::Http => write!(f, "http(s)"),
            Self::Gcs => write!(f, "gs"),
            Self::S3 => write!(f, "s3"),
        }
    }
}

impl DatasourceUrl {
    const FILE_SCHEME: &str = "file";
    const HTTP_SCHEME: &str = "http";
    const HTTPS_SCHEME: &str = "https";
    const GS_SCHEME: &str = "gs";
    const S3_SCHEME: &str = "s3";

    pub fn try_new(u: impl AsRef<str>) -> Result<Self> {
        let u = u.as_ref();

        let ds_url = match u.parse::<Url>() {
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                // It's probably a local file path.
                //
                // TODO: Check if it's actually a file path (maybe invalid but
                // probably check).
                return Ok(Self::File(PathBuf::from(u)));
            }
            Err(e) => return Err(DatasourceCommonError::InvalidUrl(e.to_string())),
            Ok(u) => u,
        };

        let ds_url = match ds_url.scheme() {
            Self::FILE_SCHEME => match ds_url.to_file_path() {
                Ok(f) => Self::File(f),
                Err(()) => {
                    return Err(DatasourceCommonError::InvalidUrl(format!(
                        "url not a valid file: {ds_url}"
                    )))
                }
            },
            Self::HTTP_SCHEME | Self::HTTPS_SCHEME | Self::GS_SCHEME | Self::S3_SCHEME => {
                Self::Url(ds_url)
            }
            other => {
                return Err(DatasourceCommonError::InvalidUrl(format!(
                    "unsupported scheme '{other}'"
                )))
            }
        };

        Ok(ds_url)
    }

    pub fn scheme(&self) -> DatasourceUrlScheme {
        match self {
            Self::File(_) => DatasourceUrlScheme::File,
            Self::Url(u) => match u.scheme() {
                Self::HTTP_SCHEME | Self::HTTPS_SCHEME => DatasourceUrlScheme::Http,
                Self::GS_SCHEME => DatasourceUrlScheme::Gcs,
                Self::S3_SCHEME => DatasourceUrlScheme::S3,
                _ => unreachable!(),
            },
        }
    }

    pub fn path(&self) -> Cow<str> {
        match self {
            Self::File(p) => p.to_string_lossy(),
            Self::Url(u) => u.path().into(),
        }
    }

    pub fn host(&self) -> Option<&str> {
        match self {
            Self::File(_) => None,
            Self::Url(u) => u.host_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_parse() {
        let u = DatasourceUrl::try_new("gs://my_bucket/my_obj").unwrap();
        assert_eq!(Some("my_bucket"), u.host());
        assert_eq!("/my_obj", u.path());
        assert_eq!(DatasourceUrlScheme::Gcs, u.scheme());

        let u = DatasourceUrl::try_new("gs://my_bucket/my_obj.parquet").unwrap();
        assert_eq!(Some("my_bucket"), u.host());
        assert_eq!("/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlScheme::Gcs, u.scheme());

        let u = DatasourceUrl::try_new("./my_bucket/my_obj.parquet").unwrap();
        assert_eq!(None, u.host());
        assert_eq!("./my_bucket/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlScheme::File, u.scheme());

        let u = DatasourceUrl::try_new("/Users/mario/my_bucket/my_obj").unwrap();
        assert_eq!(None, u.host());
        assert_eq!("/Users/mario/my_bucket/my_obj", u.path());
        assert_eq!(DatasourceUrlScheme::File, u.scheme());

        let u = DatasourceUrl::try_new("file:/my_bucket/my_obj.parquet").unwrap();
        assert_eq!("/my_bucket/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlScheme::File, u.scheme());

        // TODO: Maybe we don't want this...
        let u = DatasourceUrl::try_new("file:my_bucket/my_obj.parquet").unwrap();
        assert_eq!("/my_bucket/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlScheme::File, u.scheme());
    }
}
