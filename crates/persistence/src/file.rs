use crate::errors::{internal, Result};
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Local file cache for files stored in object storage.
#[derive(Debug)]
pub struct DiskCache {
    store: Box<dyn ObjectStore>,
    /// Path to where local files will be kept.
    local_cache_path: PathBuf,
}

impl DiskCache {
    pub fn new<O: ObjectStore>(store: O, local_cache_path: PathBuf) -> Self {
        DiskCache {
            store: Box::new(store),
            local_cache_path,
        }
    }

    pub async fn open_file<P: AsRef<Path>>(&self, relative: P) -> Result<Arc<MirroredFile>> {
        unimplemented!()
    }

    pub async fn create_file<P: AsRef<Path>>(&self, relative: P) -> Result<Arc<MirroredFile>> {
        unimplemented!()
    }

    /// Sync the local file to the remote object store.
    pub async fn sync_local<P: AsRef<Path>>(&self, relative: P) -> Result<()> {
        unimplemented!()
    }

    pub async fn remove_local<P: AsRef<Path>>(&self, relative: P) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct MirroredFileRef(Arc<MirroredFile>);

/// A file that is mirrored with a file within some remote object store.
///
/// The directory struct on the local disk mirrors the directory structure with
/// the object store.
// TODO: We'll need to update the sync stats within the relevant methods.
pub struct MirroredFile {
    obj_relative: ObjectPath,
    local_relative: PathBuf,
    local: File,
}

impl Read for MirroredFileRef {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut local = &self.0.local;
        (&mut local).read(buf)
    }
}

impl Seek for MirroredFileRef {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut local = &self.0.local;
        (&mut local).seek(pos)
    }
}

// TODO: Have the mirrored file buffer. This will also allow us to track bytes
// written to the local cache.
impl Write for MirroredFileRef {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut local = &self.0.local;
        (&mut local).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut local = &self.0.local;
        (&mut local).flush()
    }
}

impl fmt::Debug for MirroredFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MirroredFile")
            .field("path", &self.obj_relative)
            .finish()
    }
}

/// Convert a file system path to an object store path.
///
/// The provided path must be relative and canonical.
///
/// Note that this doesn't use `ObjectPath::from_filesystem_path` since that
/// function will attempt to resolve the path using the local filesystem.
fn to_object_path<P: AsRef<Path>>(path: P) -> Result<ObjectPath> {
    let path = path.as_ref();
    let s = path
        .to_str()
        .ok_or_else(|| internal!("provided path not valid utf8: {:?}", path))?;
    if path.is_absolute() {
        return Err(internal!("path must be relative: {:?}", path));
    }
    let obj_path = ObjectPath::parse(s)?;
    Ok(obj_path)
}
