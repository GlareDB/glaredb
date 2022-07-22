use anyhow::{anyhow, Result};
use lemur::repr::value::ValueVec;
use serde::{Deserialize, Serialize};
use std::io;

/// A fragment of a table stored on disk.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeapTableFragment {
    /// Row ID of the first row for this fragment.
    rowid_start: u64,
    vecs: Vec<ValueVec>,
}

impl HeapTableFragment {
    pub fn serialized_size(&self) -> Result<u64> {
        Ok(bincode::serialized_size(self)?)
    }

    pub fn serialize_into<W: io::Write>(&self, writer: W) -> Result<()> {
        bincode::serialize_into(writer, self)?;
        Ok(())
    }
}
