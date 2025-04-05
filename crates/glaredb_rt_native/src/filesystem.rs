use std::fs::{self, File as StdFile, OpenOptions};
use std::io::{ErrorKind, Read, SeekFrom};
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::{File, FileStat, FileSystem, FileType, OpenFlags};
use glaredb_error::{DbError, Result, ResultExt};

#[derive(Debug)]
pub struct LocalFile {
    len: usize,
    file: StdFile,
}

impl File for LocalFile {
    fn size(&self) -> usize {
        self.len
    }

    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let result = self.file.read(buf).context("Failed to read from file");
        Poll::Ready(result)
    }

    fn poll_write(&mut self, _buf: &mut [u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "not implemented: poll write for local file",
        )))
    }

    fn poll_seek(&mut self, _seek: SeekFrom) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new(
            "not implemented: poll seek for local file",
        )))
    }

    fn poll_flush(&mut self) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new(
            "not implemented: poll flush for local file",
        )))
    }
}

#[derive(Debug)]
pub struct LocalFileSystem {}

impl FileSystem for LocalFileSystem {
    type File = LocalFile;

    async fn open(&self, flags: OpenFlags, path: &str) -> Result<Self::File> {
        let file = OpenOptions::new()
            .read(flags.is_read())
            .write(flags.is_write())
            .create(flags.is_create())
            .open(path)?;

        let metadata = file.metadata()?;

        Ok(LocalFile {
            len: metadata.len() as usize,
            file,
        })
    }

    async fn stat(&self, path: &str) -> Result<Option<FileStat>> {
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let file_type = if metadata.is_dir() {
            FileType::Directory
        } else if metadata.is_file() {
            FileType::File
        } else {
            return Err(DbError::new("Unknown file type"));
        };

        Ok(Some(FileStat { file_type }))
    }

    fn can_handle_path(&self, _path: &str) -> bool {
        // yes
        true
    }
}
