use crate::errors::{ExpstoreError, Result};
use object_store::{path::Path as ObjectPath, ObjectStore};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use tracing::trace;

pub struct FileSync {
    store: Box<dyn ObjectStore>,
    /// Number of bytes the local cache can use.
    local_cache_size: usize,
    /// Number of bytes currently in use.
    local_cache_used: usize,
}

impl FileSync {
    pub fn new<O: ObjectStore>(store: O, local_cache_size: usize) -> Self {
        FileSync {
            store: Box::new(store),
            local_cache_size,
            local_cache_used: 0,
        }
    }
}

/// A file that is mirrored with a file within some remote object store.
pub struct MirroredFile {
    relative: PathBuf,
    local: File,
}

impl MirroredFile {
    // pub fn open(path: PathBuf)
}

impl FileExt for MirroredFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if cfg!(unix) {
            self.local.read_at(buf, offset)
        } else {
            todo!()
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        if cfg!(unix) {
            self.local.write_at(buf, offset)
        } else {
            todo!()
        }
    }
}
