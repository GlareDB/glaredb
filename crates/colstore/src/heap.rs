use anyhow::{anyhow, Result};
use log::debug;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::rc::Rc;

const OBJECT_SIZE: usize = 512 * 1024;
const OBJECT_ZERO_BUF: [u8; OBJECT_SIZE] = [0; OBJECT_SIZE];

/// A unique identifier for a file.
#[derive(Debug)]
pub struct FileId(u64);

impl FileId {
    fn as_path_buf(&self) -> PathBuf {
        PathBuf::from(self.0.to_string())
    }
}

/// A unique identifiers for an object in the heap.
///
/// Multiple objects may be colocated in the same file.
#[derive(Debug)]
pub struct HeapObjectId(FileId, u64);

/// Relative path to the lock file indicating that a process is already managing
/// this directory.
const LOCK_PATH: &str = "lock";
/// Relative path to the metadata file.
const METADATA_PATH: &str = "metadata";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Metadata {
    num_heap_files: u64,
}

#[derive(Debug)]
struct MetadataWriter {
    file: File,
    metadata: Metadata,
}

impl MetadataWriter {
    fn open_or_create<P: AsRef<Path>>(path: P) -> Result<MetadataWriter> {
        let path = path.as_ref().join(METADATA_PATH);
        if path.exists() {
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let m: Metadata = bincode::deserialize(&buf)?;
            Ok(MetadataWriter { file, metadata: m })
        } else {
            let m = Metadata::default();
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            bincode::serialize_into(&mut file, &m)?;
            file.sync_all()?;
            Ok(MetadataWriter { file, metadata: m })
        }
    }

    /// Increment the tracked number of heap files, returning the previous
    /// value.
    fn inc_num_heap_files(&mut self) -> u64 {
        let n = self.metadata.num_heap_files;
        self.metadata.num_heap_files += 1;
        n
    }

    fn write_flush(&mut self) -> Result<()> {
        bincode::serialize_into(&self.file, &self.metadata)?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Heap {
    path: PathBuf,
    md: RwLock<MetadataWriter>,
}

impl Heap {
    /// Open a heap at the given directory.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Heap> {
        let path = path.as_ref();
        debug!("opening heap at {:?}", path);
        match fs::read_dir(path) {
            Ok(_) => debug!("reusing heap directory"),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                debug!("creating heap directory");
                fs::create_dir_all(path)?;
            }
            Err(e) => return Err(e.into()),
        }
        unimplemented!()
    }

    /// Allocate a file which may hold some number of objects. The contents of
    /// the file is undefined.
    pub fn allocate_file(&self, num_objects: usize) -> Result<FileId> {
        let mut md = self.md.write(); // TODO: We can probably drop this earlier.
        let file_id = FileId(md.inc_num_heap_files());

        let path = self.path.join(file_id.as_path_buf());
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;

        for _i in 0..num_objects {
            file.write_all(&OBJECT_ZERO_BUF)?;
        }

        // TODO: Probably sync dir too.
        file.sync_all()?;

        Ok(file_id)
    }
}
