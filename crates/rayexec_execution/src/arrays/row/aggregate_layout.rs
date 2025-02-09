use super::row_layout::RowLayout;
use crate::arrays::datatype::DataType;
use crate::functions::aggregate::states::AggregateFunctionImpl;

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

impl AggregateLayout {
    pub fn new<'a>(
        group_types: impl IntoIterator<Item = DataType>,
        aggregates: impl IntoIterator<Item = AggregateInfo>,
    ) -> Self {
        let groups = RowLayout::new(group_types);

        unimplemented!()
    }
}

/// Compute the padding needed to ensure alignment.
const fn align_pad(curr_len: usize, alignment: usize) -> usize {
    (alignment - (curr_len % alignment)) % alignment
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_pad_sanity() {
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
                expect: 2,
            },
            TestCase {
                curr_len: 13,
                alignment: 4,
                expect: 3,
            },
        ];

        for tc in test_cases {
            let got = align_pad(tc.curr_len, tc.alignment);
            assert_eq!(tc.expect, got);
        }
    }
}
