use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
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

#[cfg(test)]
mod tests {
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
}
