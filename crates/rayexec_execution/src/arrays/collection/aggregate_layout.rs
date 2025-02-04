use super::row_layout::RowLayout;

#[derive(Debug, Clone)]
pub struct AggregateInfo {
    /// Alignment requirement for the aggregate.
    pub align: usize,
    /// Size in bytes for the aggregate state.
    pub size: usize,
}

#[derive(Debug)]
pub struct AggregateLayout {
    /// Layout for the groups part of the aggregate.
    pub(crate) groups: RowLayout,
    /// Aggregates for this layout.
    pub(crate) aggregates: Vec<AggregateInfo>,
}

/// Compute the padding needed to ensure alignment.
const fn align_pad(curr_len: usize, alignment: usize) -> usize {
    (alignment - (curr_len % alignment)) % alignment
}
