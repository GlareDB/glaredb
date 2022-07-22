//! Write bincode encoded objects out to files.
use anyhow::{anyhow, Result};
use log::{debug, warn};
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{btree_map::Entry, BTreeMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A file identifier. A single identifier may point to multiple versions of the
/// same file.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileId {
    pub name: String,
}

/// Metadata for a file.
///
/// Every write to a file produces a new version. A file ID and a version points
/// to a unique file on the filesystem.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileMetadata {
    pub num_chunks: u64,
    pub version: u64,
}

/// Relative path to the lock file indicating that a process is already managing
/// this directory.
const LOCK_PATH: &str = "lock";

#[derive(Debug)]
struct HeapFile {
    file: File,
    version: u64,
}

#[derive(Debug)]
pub struct Heap {
    path: PathBuf,
    tmp_counter: AtomicU64,
    files: RwLock<BTreeMap<FileId, HeapFile>>,
}

impl Heap {
    /// Open a heap at the given directory.
    ///
    /// Create the directory if it doesn't exist.
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

        // let mut files = BTreeMap::new();
        let mut max = 0;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or(anyhow!("file name not a utf-8 string: {:?}", name,))?;
            debug!("reading heap file: {}", name);

            if name == LOCK_PATH {
                return Err(anyhow!("lock file exists"));
            }

            if name.ends_with("tmp") {
                debug!("removing temp file");
                fs::remove_file(entry.path())?;
                continue;
            }

            let parts: Vec<_> = name.split("-").collect();
            if parts.len() != 2 {
                return Err(anyhow!("unexpected filename in heap directory: {}", name));
            }

            let id = parts[0].parse::<u64>()?;
            let version = parts[1].parse::<u64>()?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(entry.path())?;

            if id > max {
                max = id;
            }

            // let id = FileId(id);
            // let heapfile = HeapFile {
            //     file,
            //     id: id.clone(),
            //     version,
            // };

            // TODO: How to handle duplicate versions?
            // match files.entry(id) {
            //     Entry::Vacant(entry) => {
            //         entry.insert(heapfile);
            //     }
            //     Entry::Occupied(mut entry) => {
            //         if entry.get().version < version {
            //             entry.insert(heapfile);
            //         }
            //     }
            // }
        }

        debug!("creating lock file");
        let _ = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path.join(LOCK_PATH))?;

        unimplemented!()
        // Ok(Heap {
        //     path: path.into(),
        //     tmp_counter: AtomicU64::new(0),
        //     next_file_id: AtomicU64::new(max + 1),
        //     files: RwLock::new(files),
        // })
    }

    pub fn close(self) -> Result<()> {
        debug!("closing heap");
        let files = self.files.write();
        for (_, file) in files.iter() {
            file.file.sync_all()?;
        }
        fs::remove_file(self.path.join(LOCK_PATH))?;

        Ok(())
    }

    /// Allocate a new file with some initial data. This initial data will be
    /// considered the first chunk of the file.
    ///
    /// Errors if a file for that ID already exists.
    pub fn allocate<S: Serialize>(&self, id: &FileId, data: &S) -> Result<()> {
        let version = 0;
        let name = format!("{}-{}", id.name, version);
        debug!("allocating new file: {}", name);

        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.path.join(&name))?;

        // Write out initial number of chunks (1).
        file.write_all(&u64::to_le_bytes(1))?;

        // Write first chunk.
        bincode::serialize_into(&mut file, data)?;

        file.sync_all()?;

        let mut files = self.files.write();
        let heapfile = HeapFile { file, version };
        files.insert(id.clone(), heapfile);

        Ok(())
    }

    /// Write chunks to the given file.
    pub fn write_chunks<'a, S: 'a, I>(&self, id: &FileId, chunks: I) -> Result<u64>
    where
        S: Serialize,
        I: IntoIterator<Item = &'a S>,
    {
        unimplemented!()
    }

    /// Overwrite the contents for some file.
    ///
    /// Errors if there's a concurrent overwrite happening for the same file.
    pub fn overwrite<B: Serialize>(&self, id: FileId, data: &B) -> Result<()> {
        let tmp_name = format!("{}.tmp", self.tmp_counter.fetch_add(1, Ordering::Relaxed));
        let mut tmp_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.path.join(&tmp_name))?;

        bincode::serialize_into(&mut tmp_file, data)?;

        let version = {
            let files = self.files.read();
            let f = files
                .get(&id)
                .ok_or(anyhow!("missing file for id: {:?}", id))?;
            f.version
        };

        tmp_file.sync_all()?;

        let new_version = version + 1;
        let dest_name = format!("{}-{}", "hello", new_version);
        fs::rename(self.path.join(&tmp_name), self.path.join(&dest_name))?;

        let new_file = OpenOptions::new()
            .read(true)
            .open(self.path.join(&dest_name))?;

        // Swap out file handles.
        let mut files = self.files.write();
        let file = files
            .get_mut(&id)
            .ok_or(anyhow!("missing file for id: {:?}", id))?;
        if file.version != version {
            return Err(anyhow!("concurrent overwrites for {:?}", id));
        }
        file.version = new_version;
        let _ = std::mem::replace(&mut file.file, new_file);

        // TODO: Delete original file.

        Ok(())
    }

    /// Read the contents of some file.
    pub fn read<B: DeserializeOwned>(&self, id: FileId) -> Result<Option<B>> {
        let files = self.files.read();
        let file = match files.get(&id) {
            Some(file) => file,
            None => return Ok(None),
        };

        let reader = &mut (&file.file);
        let data = bincode::deserialize_from(reader)?;

        Ok(Some(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn simple_read_write() {
        logutil::init_test();

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct TestStruct {
            value: String,
        }

        // let path = tempdir().unwrap().into_path();
        // let heap = Heap::open(path).unwrap();

        // let fid = heap.allocate_file().unwrap();
        // let obj = TestStruct {
        //     value: "hello world".to_string(),
        // };
        // heap.overwrite(fid, &obj).unwrap();

        // let out: TestStruct = heap.read(fid).unwrap().unwrap();
        // assert_eq!(obj, out);
    }

    #[test]
    fn errors_on_multiple_open() {
        // logutil::init_test();

        // let path = tempdir().unwrap().into_path();

        // let heap1 = Heap::open(&path).unwrap();
        // let _ = Heap::open(&path).expect_err("lock file should prevent open");
    }
}
