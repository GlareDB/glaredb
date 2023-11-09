//! IO and formatting related utilies.

use std::path::{Path, PathBuf};
pub mod fmt;
pub mod write;

pub fn resolve_path(path: &Path) -> std::io::Result<PathBuf> {
    if path.starts_with("~/") {
        if let Some(homedir) = home::home_dir() {
            return Ok(homedir.join(path.strip_prefix("~/").unwrap()));
        }
    }
    path.canonicalize().map(|p| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_path_with_homedir() {
        assert_eq!(
            resolve_path(Path::new("~/foo/bar")).unwrap(),
            home::home_dir().unwrap().join("foo/bar")
        );
    }

    #[test]
    fn fails_to_resolve_nonexistent_path() {
        assert!(resolve_path(Path::new("/foo/bar")).is_err());
    }
}
