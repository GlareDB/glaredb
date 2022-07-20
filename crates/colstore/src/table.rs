use anyhow::{anyhow, Result};
use lemur::repr::value::ValueVec;

#[derive(Debug)]
pub struct TableFragment {
    rowid: u64,
    vecs: Vec<ValueVec>,
}
