use std::fs;
use std::path::{Path, PathBuf};

use glaredb_error::{DbError, Result, ResultExt};

/// Recursively find all files ending with the given extension in path.
///
/// If the provided path points to a file, then the returned vec will only
/// contain a single path buf pointing to that file.
pub fn find_files(path: &Path, ext: &str) -> Result<Vec<PathBuf>> {
    fn inner(dir: &Path, paths: &mut Vec<PathBuf>, ext: &str) -> Result<()> {
        if dir.is_dir() {
            let readdir = fs::read_dir(dir).context("Failed to read directory")?;
            for entry in readdir {
                let entry = entry.context("Failed to get entry")?;
                let path = entry.path();

                let path_str = path
                    .to_str()
                    .ok_or_else(|| DbError::new("Expected utf8 path"))?;
                // We might have READMEs interspersed. Only read .bench files.
                if path.is_file() && !path_str.ends_with(ext) {
                    continue;
                }

                if path.is_dir() {
                    inner(&path, paths, ext)?;
                } else {
                    paths.push(path.to_path_buf());
                }
            }
        }
        Ok(())
    }

    if path.is_file() {
        let path_str = path
            .to_str()
            .ok_or_else(|| DbError::new("Expected utf8 path"))?;
        if !path_str.ends_with(ext) {
            return Err(DbError::new(format!(
                "Provided file '{path_str}' doesn't end with extension '{ext}'"
            )));
        }

        return Ok(vec![path.to_path_buf()]);
    }

    let mut paths = Vec::new();
    inner(path, &mut paths, ext)?;

    Ok(paths)
}
