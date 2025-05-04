use super::sort_layout::SortLayout;
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
