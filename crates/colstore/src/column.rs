use anyhow::{anyhow, Result};
use lemur::repr::value::ValueVec;

/// A partial column starting at some row id.
#[derive(Debug)]
pub struct ColumnFragment {
    rowid: u64,
    vec: ValueVec,
}

impl ColumnFragment {
    pub fn get_rowid(&self) -> u64 {
        self.rowid
    }
}
