use super::row_layout::RowLayout;
use crate::arrays::datatype::DataType;

#[derive(Debug, Clone)]
pub struct AggregateInfo {
    /// Alignment requirement for the aggregate.
    pub align: usize,
    /// Size in bytes for the aggregate state.
    pub size: usize,
}

#[derive(Debug)]
pub struct AggregateLayout {
    /// Required base alignment for a buffer holding this aggregate layout.
    ///
    /// Every row will be aligned to this value which may mean that there exists
    /// padding at the end of a row.
    ///
    /// Every aggregate will by locally aligned within its row to allow for
    /// aligned reads and writes for in place updates.
    ///
    /// Group values are not guaranteed to be aligned, and should be
    /// written/read with bitwise copies.
    pub(crate) base_align: usize,
    /// Layout for the groups part of the aggregate.
    pub(crate) groups: RowLayout,
    /// Aggregates for this layout.
    pub(crate) aggregates: Vec<AggregateInfo>,
    /// Row width in bytes of both the group values and the aggregates.
    pub(crate) row_width: usize,
    /// Byte offsets to the aggregates in the row.
    ///
    /// These will be aligned to the aggregate object.
    pub(crate) aggregate_offsets: Vec<usize>,
}

impl AggregateLayout {
    /// Create a new layout representing a row of group values and aggregates.
    pub fn new<'a>(
        group_types: impl IntoIterator<Item = DataType>,
        aggregates: impl IntoIterator<Item = AggregateInfo>,
    ) -> Self {
        let groups = RowLayout::new(group_types);
        let aggregates: Vec<_> = aggregates.into_iter().collect();

        let base_align: usize = aggregates.iter().map(|info| info.align).max().unwrap_or(1);

        let offset = groups.row_width;
        let mut offset = align_len(offset, base_align);

        let mut aggregate_offsets = Vec::with_capacity(aggregates.len());

        for agg in &aggregates {
            aggregate_offsets.push(offset);
            offset += agg.size;
            // TODO: Could be more efficient here and align to the aggregate
            // itself.
            offset = align_len(offset, base_align);
        }

        // Ensure next row in the buffer matches up with the base alignment.
        let row_width = align_len(offset, base_align);

        AggregateLayout {
            base_align,
            groups,
            aggregates,
            row_width,
            aggregate_offsets,
        }
    }
}

/// Compute the new len to ensure alignment to some value.
const fn align_len(curr_len: usize, alignment: usize) -> usize {
    assert!(alignment != 0, "alignment cannot be zero");
    ((curr_len + (alignment - 1)) / alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_len_sanity() {
        struct TestCase {
            curr_len: usize,
            alignment: usize,
            expect: usize,
        }

        let test_cases = [
            TestCase {
                curr_len: 0,
                alignment: 4,
                expect: 0,
            },
            TestCase {
                curr_len: 2,
                alignment: 4,
                expect: 4,
            },
            TestCase {
                curr_len: 13,
                alignment: 4,
                expect: 16,
            },
        ];

        for tc in test_cases {
            let got = align_len(tc.curr_len, tc.alignment);
            assert_eq!(tc.expect, got);
        }
    }

    #[test]
    fn new_no_groups() {
        let layout = AggregateLayout::new(
            [],
            [
                AggregateInfo { align: 2, size: 4 },
                AggregateInfo { align: 1, size: 1 },
            ],
        );

        assert_eq!(2, layout.base_align);
        assert_eq!(0, layout.aggregate_offsets[0]);
        assert_eq!(4, layout.aggregate_offsets[1]);
        assert_eq!(6, layout.row_width); // Read width is 5, +1 to ensure alignment to 2
    }
}
