use std::fs::{self, File as StdFile, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_core::runtime::filesystem::{
    FileHandle,
    FileOpenContext,
    FileStat,
    FileSystem,
    FileType,
    OpenFlags,
};
use glaredb_error::{DbError, Result, ResultExt};

#[derive(Debug)]
pub struct LocalFileHandle {
    path: String,
    len: usize,
    file: StdFile,
}

impl FileHandle for LocalFileHandle {
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
    type FileHandle = LocalFileHandle;
    type ReadDirHandle = LocalDirHandle;
    type State = ();

    fn state_from_context(&self, _context: FileOpenContext) -> Result<Self::State> {
        Ok(())
    }

    async fn open(&self, flags: OpenFlags, path: &str, _state: &()) -> Result<Self::FileHandle> {
        let file = OpenOptions::new()
            .read(flags.is_read())
            .write(flags.is_write())
            .create(flags.is_create())
            .open(path)?;

        let metadata = file.metadata()?;

        Ok(LocalFileHandle {
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

    fn read_dir(&self, dir: &str, _state: &Self::State) -> Self::ReadDirHandle {
        LocalDirHandle {
            path: dir.into(),
            exhausted: false,
        }
    }

    fn can_handle_path(&self, _path: &str) -> bool {
        // yes
        true
    }
}

#[derive(Debug)]
pub struct LocalDirHandle {
    path: PathBuf,
    exhausted: bool,
}

impl LocalDirHandle {
    fn list_inner(&mut self, ents: &mut Vec<DirEntry>) -> Result<usize> {
        if self.exhausted {
            return Ok(0);
        }

        let ents_len = ents.len();

        let readdir = fs::read_dir(&self.path)
            .context_fn(|| format!("Failed to read directory: {}", self.path.to_string_lossy(),))?;

        for entry in readdir {
            let entry = entry.context("Failed to get entry")?;
            let path = entry.path();
            if path.is_dir() {
                ents.push(DirEntry {
                    path: path.to_string_lossy().to_string(),
                    file_type: FileType::Directory,
                })
            } else {
                ents.push(DirEntry {
                    path: path.to_string_lossy().to_string(),
                    file_type: FileType::File,
                })
            }
        }

        let appended = ents.len() - ents_len;
        self.exhausted = true;

        Ok(appended)
    }
}

impl ReadDirHandle for LocalDirHandle {
    fn poll_list(&mut self, _cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        Poll::Ready(self.list_inner(ents))
    }

    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self> {
        let new_path = self.path.join(relative.into());
        Ok(LocalDirHandle {
            path: new_path,
            exhausted: false,
        })
    }
}
