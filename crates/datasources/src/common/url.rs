//! Utility for source "URLs".

use std::{borrow::Cow, fmt::Display, path::PathBuf};

use datafusion::{datasource::file_format::file_type::FileCompressionType, scalar::ScalarValue};
use datafusion_ext::{
    errors::ExtensionError,
    functions::{FromFuncParamValue, FuncParamValue},
};
use url::Url;

use super::errors::{DatasourceCommonError, Result};

/// Describes the type of a data source url.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasourceUrlType {
    File,
    Http,
    Gcs,
    S3,
}

impl Display for DatasourceUrlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File => write!(f, "file"),
            Self::Http => write!(f, "http(s)"),
            Self::Gcs => write!(f, "gs"),
            Self::S3 => write!(f, "s3"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatasourceUrl {
    File(PathBuf),
    Url(Url),
}

impl Display for DatasourceUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(p) => write!(f, "{}", p.to_string_lossy()),
            Self::Url(u) => write!(f, "{u}"),
        }
    }
}

impl FromFuncParamValue for DatasourceUrl {
    fn from_param(value: FuncParamValue) -> datafusion_ext::errors::Result<Self> {
        let url_string: String = value.param_into()?;
        Self::try_new(&url_string).map_err(|_e| ExtensionError::InvalidParamValue {
            param: url_string,
            expected: "datasource url",
        })
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        match value {
            FuncParamValue::Scalar(ScalarValue::Utf8(Some(s))) => Self::try_new(s).is_ok(),
            _ => false,
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

    pub fn datasource_url_type(&self) -> DatasourceUrlType {
        match self {
            Self::File(_) => DatasourceUrlType::File,
            Self::Url(u) => match u.scheme() {
                Self::HTTP_SCHEME | Self::HTTPS_SCHEME => DatasourceUrlType::Http,
                Self::GS_SCHEME => DatasourceUrlType::Gcs,
                Self::S3_SCHEME => DatasourceUrlType::S3,
                _ => unreachable!(),
            },
        }
    }

    pub fn scheme(&self) -> &str {
        match self {
            Self::File(_) => Self::FILE_SCHEME,
            Self::Url(u) => u.scheme(),
        }
    }

    pub fn path(&self) -> Cow<str> {
        match self {
            Self::File(p) => p.to_string_lossy(),
            Self::Url(u) => u.path().trim_start_matches('/').into(),
        }
    }

    pub fn host(&self) -> Option<&str> {
        match self {
            Self::File(_) => None,
            Self::Url(u) => u.host_str(),
        }
    }

    pub fn as_url(&self) -> Result<Url> {
        match self {
            Self::File(p) if p.is_absolute() => {
                Ok(format!("file:{}", p.to_string_lossy()).parse()?)
            }
            Self::Url(u) => Ok(u.clone()),
            _ => Err(DatasourceCommonError::InvalidUrl(
                "cannot convert datasource URL to a generic URL".to_string(),
            )),
        }
    }

    pub fn get_file_compression(&self) -> FileCompressionType {
        match self {
            DatasourceUrl::File(f) => f
                .extension()
                .and_then(|s| s.to_str())
                .and_then(|ext| ext.parse().ok())
                .unwrap_or(FileCompressionType::UNCOMPRESSED),
            DatasourceUrl::Url(u) => u
                .path()
                .rsplit_once('.')
                .and_then(|(_, ext)| ext.parse().ok())
                .unwrap_or(FileCompressionType::UNCOMPRESSED),
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
        assert_eq!("my_obj", u.path());
        assert_eq!(DatasourceUrlType::Gcs, u.datasource_url_type());

        let u = DatasourceUrl::try_new("gs://my_bucket/my_obj.parquet").unwrap();
        assert_eq!(Some("my_bucket"), u.host());
        assert_eq!("my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlType::Gcs, u.datasource_url_type());

        let u = DatasourceUrl::try_new("./my_bucket/my_obj.parquet").unwrap();
        assert_eq!(None, u.host());
        assert_eq!("./my_bucket/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlType::File, u.datasource_url_type());

        let u = DatasourceUrl::try_new("/Users/mario/my_bucket/my_obj").unwrap();
        assert_eq!(None, u.host());
        assert_eq!("/Users/mario/my_bucket/my_obj", u.path());
        assert_eq!(DatasourceUrlType::File, u.datasource_url_type());

        let u = DatasourceUrl::try_new("file:/my_bucket/my_obj.parquet").unwrap();
        assert_eq!("/my_bucket/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlType::File, u.datasource_url_type());

        let u = DatasourceUrl::try_new("file:my_bucket/my_obj.parquet").unwrap();
        assert_eq!("/my_bucket/my_obj.parquet", u.path());
        assert_eq!(DatasourceUrlType::File, u.datasource_url_type());
    }
}
