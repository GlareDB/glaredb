#![allow(unused)]

use super::column_reader::ColumnReader;

#[derive(Debug)]
pub struct StructReader {
    pub(crate) readers: Vec<ColumnReader>,
}
