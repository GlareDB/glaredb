use bitvec::slice::BitSlice;
use std::ops::Range;

/// The permutation of a sort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortPermutation(Vec<usize>);

impl SortPermutation {
    /// Return a permuation that would maintain the order of elements for a
    /// slice of some length.
    pub fn same_order(len: usize) -> Self {
        SortPermutation((0..len).collect())
    }

    /// Return a permutation that would reverse the order of some slice.
    pub fn reverse_order(len: usize) -> Self {
        let mut idxs = (0..len).collect::<Vec<_>>();
        idxs.reverse();
        SortPermutation(idxs)
    }

    /// Compute a permutation that would sort `slice`.
    pub fn from_slice<T, S>(slice: &S) -> Self
    where
        T: Ord,
        S: AsRef<[T]> + ?Sized,
    {
        let slice = slice.as_ref();
        let mut p = SortPermutation((0..slice.len()).collect());
        p.0.sort_by_key(|&idx| &slice[idx]);
        p
    }

    /// Apply this permutation to a slice.
    ///
    /// Panics if the lengths do not match.
    pub fn apply_to<T, S>(&self, slice: &mut S)
    where
        S: AsMut<[T]> + ?Sized,
    {
        let slice = slice.as_mut();
        assert_eq!(
            self.0.len(),
            slice.len(),
            "permuation and slice lengths must match"
        );

        let mut indices = self.0.clone();

        for idx in 0..slice.len() {
            if indices[idx] != idx {
                let mut curr_idx = idx;
                loop {
                    let target_idx = indices[curr_idx];
                    indices[curr_idx] = curr_idx;
                    if indices[target_idx] == target_idx {
                        break;
                    }
                    slice.swap(curr_idx, target_idx);
                    curr_idx = target_idx;
                }
            }
        }
    }

    /// Specialized version of `apply_to` to work with bit slices.
    pub fn apply_to_bitslice(&self, slice: &mut BitSlice) {
        assert_eq!(
            self.0.len(),
            slice.len(),
            "permuation and slice lengths must match"
        );

        let mut indices = self.0.clone();

        for idx in 0..slice.len() {
            if indices[idx] != idx {
                let mut curr_idx = idx;
                loop {
                    let target_idx = indices[curr_idx];
                    indices[curr_idx] = curr_idx;
                    if indices[target_idx] == target_idx {
                        break;
                    }
                    slice.swap(curr_idx, target_idx);
                    curr_idx = target_idx;
                }
            }
        }
    }
}

/// Ranges for unique groups in a sorted vector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRanges {
    pub ranges: Vec<Range<usize>>,
}

impl GroupRanges {
    /// Create a group range representing no groups.
    pub fn no_group() -> Self {
        GroupRanges { ranges: Vec::new() }
    }

    /// Create a group range representing a vector of some length as a single
    /// group.
    pub fn single_group(len: usize) -> Self {
        GroupRanges {
            ranges: vec![Range { start: 0, end: len }],
        }
    }

    /// Return the number of groups.
    pub fn num_groups(&self) -> usize {
        self.ranges.len()
    }

    /// Generate groups from a sorted slice.
    ///
    /// Correctness is only guaranteed for sorted slices.
    pub fn from_slice<T>(slice: &[T]) -> Self
    where
        T: PartialEq,
    {
        let mut ranges = Vec::new();

        let mut iter = slice.iter();
        let mut cmp_val = match iter.next() {
            Some(val) => val,
            None => return GroupRanges { ranges },
        };

        let mut curr_range = Range { start: 0, end: 1 };

        for val in iter {
            if val == cmp_val {
                curr_range.end += 1;
            } else {
                cmp_val = val;
                let new_start = curr_range.end;
                ranges.push(curr_range);
                curr_range = Range {
                    start: new_start,
                    end: new_start + 1,
                };
            }
        }
        ranges.push(curr_range);

        GroupRanges { ranges }
    }

    /// Append some other group range to this group range.
    ///
    /// The ranges in other will have a constant offset added to all starts and
    /// ends to make the combined set of ranges contiguous with not overlaps.
    pub fn make_contiguous(&mut self, other: GroupRanges) {
        let offset = self.last_end().unwrap_or(0);

        self.ranges
            .extend(other.ranges.into_iter().map(|range| Range {
                start: range.start + offset,
                end: range.end + offset,
            }))
    }

    pub fn last_end(&self) -> Option<usize> {
        Some(self.ranges.last()?.end)
    }

    pub fn iter_lens(&self) -> impl Iterator<Item = usize> + '_ {
        self.ranges.iter().map(|r| r.end - r.start)
    }

    pub fn iter_ranges(&self) -> impl Iterator<Item = &Range<usize>> + '_ {
        self.ranges.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permute_sort_simple() {
        let mut s = vec![4, 3, 5, 1, 2];
        let perm = SortPermutation::from_slice(&s);
        perm.apply_to(&mut s);

        assert_eq!(vec![1, 2, 3, 4, 5], s);
    }

    #[test]
    fn group_ranges_simple() {
        let s = vec![1, 1, 2, 3, 3, 3];
        let ranges = GroupRanges::from_slice(&s);

        let expected = GroupRanges {
            ranges: vec![(0..2), (2..3), (3..6)],
        };

        assert_eq!(expected, ranges);
    }

    #[test]
    fn make_contiguous() {
        let mut g1 = GroupRanges::single_group(3);
        let g2 = GroupRanges::single_group(4);

        g1.make_contiguous(g2);

        assert_eq!(7, g1.last_end().unwrap());
        let lens: Vec<_> = g1.iter_lens().collect();
        assert_eq!(vec![3, 4], lens);
    }
}
