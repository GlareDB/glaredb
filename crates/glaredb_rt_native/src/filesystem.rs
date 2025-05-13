use std::fs::{self, File as StdFile, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::file_list::{DirList, NotImplementedDirList};
use glaredb_core::runtime::filesystem::{
    File,
    FileOpenContext,
    FileStat,
    FileSystem,
    FileType,
    OpenFlags,
};
use glaredb_error::{DbError, Result, ResultExt};

#[derive(Debug)]
pub struct LocalFile {
    path: String,
    len: usize,
    file: StdFile,
}

impl File for LocalFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn size(&self) -> usize {
        self.len
    }

    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let result = self.file.read(buf).context("Failed to read from file");
        Poll::Ready(result)
    }

    fn poll_write(&mut self, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "not implemented: poll write for local file",
        )))
    }

    fn poll_seek(&mut self, _cx: &mut Context, seek: SeekFrom) -> Poll<Result<()>> {
        Poll::Ready(self.file.seek(seek).context("Failed to seek").map(|_| ()))
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Err(DbError::new(
            "not implemented: poll flush for local file",
        )))
    }
}

#[derive(Debug)]
pub struct LocalFileSystem {}

impl FileSystem for LocalFileSystem {
    type File = LocalFile;
    type State = ();
    type DirList = LocalDirList;

    fn state_from_context(&self, _context: FileOpenContext) -> Result<Self::State> {
        Ok(())
    }

    async fn open(&self, flags: OpenFlags, path: &str, _state: &()) -> Result<Self::File> {
        let file = OpenOptions::new()
            .read(flags.is_read())
            .write(flags.is_write())
            .create(flags.is_create())
            .open(path)?;

        let metadata = file.metadata()?;

        Ok(LocalFile {
            path: path.to_string(),
            len: metadata.len() as usize,
            file,
        })
    }

    async fn stat(&self, path: &str, _state: &()) -> Result<Option<FileStat>> {
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

    fn read_dir(&self, prefix: &str, _state: &Self::State) -> Self::DirList {
        LocalDirList {
            root: PathBuf::from(prefix),
            exhausted: false,
        }
    }

    fn can_handle_path(&self, _path: &str) -> bool {
        // yes
        true
    }
}

#[derive(Debug)]
pub struct LocalDirList {
    root: PathBuf,
    exhausted: bool,
}

impl LocalDirList {
    fn list_inner(&mut self, paths: &mut Vec<String>) -> Result<usize> {
        if self.exhausted {
            return Ok(0);
        }

        fn inner(dir: &Path, paths: &mut Vec<String>) -> Result<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir).context("Failed to read directory")? {
                    let entry = entry.context("Failed to get entry")?;
                    let path = entry.path();
                    if path.is_dir() {
                        inner(&path, paths)?;
                    } else {
                        paths.push(path.to_string_lossy().to_string());
                    }
                }
            }
            Ok(())
        }

        let n = paths.len();
        inner(&self.root, paths)?;
        self.exhausted = true;

        Ok(paths.len() - n)
    }
}

impl DirList for LocalDirList {
    fn poll_list(&mut self, _cx: &mut Context, paths: &mut Vec<String>) -> Poll<Result<usize>> {
        Poll::Ready(self.list_inner(paths))
    }
}
