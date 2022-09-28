use crate::errors::Result;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub struct DataFile {
    file: File,
}

impl DataFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DataFile> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(DataFile { file })
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}
