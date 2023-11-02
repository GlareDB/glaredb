//! IO and formatting related utilies.

use std::path::{Path, PathBuf};
pub mod fmt;
pub mod write;

pub fn resolve_path(path: &Path) -> std::io::Result<PathBuf> {
    if path.starts_with("~") {
        if let Some(homedir) = home::home_dir() {
            return Ok(homedir.join(path.strip_prefix("~").unwrap()));
        }
    }

    path.canonicalize().map(|p| p.to_path_buf())
}
