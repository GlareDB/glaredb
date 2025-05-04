use std::borrow::Borrow;

use glaredb_error::Result;

use super::sort_layout::SortLayout;
use crate::arrays::array::Array;
use crate::arrays::row::row_layout::RowLayout;

#[derive(Debug)]
pub struct TopKHeapAppendState {}

#[derive(Debug)]
pub struct TopKHeap {
    /// Layout for the sorting keys.
    key_layout: SortLayout,
    /// layout for data that's not part of the sorting key.
    data_layout: RowLayout,
}

impl TopKHeap {
    pub fn append<A>(
        &mut self,
        state: &mut TopKHeapAppendState,
        keys: &[A],
        data: &[A],
        count: usize,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        unimplemented!()
    }
}
