use std::ops::Range;

/// Maps a logical row index to the physical location in the array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Vec<usize>,
}

impl Default for SelectionVector {
    fn default() -> Self {
        Self::empty()
    }
}

impl SelectionVector {
    /// Create a new empty selection vector. Logically this means an array has
    /// no rows even if the array physically contains data.
    pub const fn empty() -> Self {
        SelectionVector {
            indices: Vec::new(),
        }
    }

    /// Create an empty selection vector with capacity.
    pub fn with_capacity(cap: usize) -> Self {
        SelectionVector {
            indices: Vec::with_capacity(cap),
        }
    }

    /// Creates a selection vector that that has all indices in the range [0,n)
    /// point to the same physical index.
    pub fn repeated(len: usize, idx: usize) -> Self {
        SelectionVector {
            indices: vec![idx; len],
        }
    }

    /// Create a selection vector with a linear mapping to a range of rows.
    pub fn with_range(range: Range<usize>) -> Self {
        SelectionVector {
            indices: range.collect(),
        }
    }

    /// Try to get the location of an index, returning None if the index is out
    /// of bounds.
    pub fn get(&self, idx: usize) -> Option<usize> {
        self.indices.get(idx).copied()
    }

    /// Get the location of a logical index.
    ///
    /// Panics if `idx` is out of bounds.
    #[inline]
    pub fn get_unchecked(&self, idx: usize) -> usize {
        self.indices[idx]
    }

    /// Sets the location for a logical index.
    ///
    /// Panics if `idx` is out of bounds.
    pub fn set_unchecked(&mut self, idx: usize, location: usize) {
        self.indices[idx] = location
    }

    pub fn slice_unchecked(&self, offset: usize, count: usize) -> Self {
        let indices = self.indices[offset..(offset + count)].to_vec();
        SelectionVector { indices }
    }

    /// Selects indices from this selection vector using some other selection
    /// vector.
    ///
    /// OUT[IDX] = SELF[SELECTION[IDX]]
    pub fn select(&self, selection: &SelectionVector) -> Self {
        let mut new_indices = Vec::with_capacity(selection.num_rows());
        for loc in selection.iter_locations() {
            let orig_loc = self.get_unchecked(loc);
            new_indices.push(orig_loc);
        }

        SelectionVector {
            indices: new_indices,
        }
    }

    /// Clear the selection vector.
    pub fn clear(&mut self) {
        self.indices.clear()
    }

    /// Returns an iterator of locations being pointed to.
    ///
    /// Locations are iterated in their logical ordering, so the resulting
    /// iterator may produce locations out of order and/or duplicated.
    ///
    /// For example, a constant vector of length '3' pointing to physical
    /// location '1' will return '1' 3 times.
    pub fn iter_locations(&self) -> impl Iterator<Item = usize> + '_ {
        self.indices.iter().copied()
    }

    pub fn num_rows(&self) -> usize {
        self.indices.len()
    }

    /// Pushes a location to the next logical index.
    ///
    /// Crate visibility since this is specific to generating selection vectors
    /// using the select executor.
    pub(crate) fn push_location(&mut self, location: usize) {
        self.indices.push(location)
    }
}

impl FromIterator<usize> for SelectionVector {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Self {
        SelectionVector {
            indices: iter.into_iter().collect(),
        }
    }
}

impl Extend<usize> for SelectionVector {
    fn extend<T: IntoIterator<Item = usize>>(&mut self, iter: T) {
        self.indices.extend(iter)
    }
}

/// Gets the physical row index for a logical index.
///
/// If `selection` is None, the index maps directly to the physical location.
#[inline]
pub fn get_unchecked(selection: Option<&SelectionVector>, idx: usize) -> usize {
    match selection {
        Some(s) => s.get_unchecked(idx),
        None => idx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_select_simple() {
        // 0 => 4
        // 1 => 5
        // 2 => 6
        // 3 => 10
        // 4 => 11
        let orig = SelectionVector::from_iter([4, 5, 6, 10, 11]);

        // 0 => 1
        // 1 => 2
        // 2 => 4
        let selection = SelectionVector::from_iter([1, 2, 4]);

        // 0 => ORIG[1] => 5
        // 1 => ORIG[2] => 6
        // 2 => ORIG[4] => 11
        let out = orig.select(&selection);

        assert_eq!(Some(5), out.get(0));
        assert_eq!(Some(6), out.get(1));
        assert_eq!(Some(11), out.get(2));
        assert_eq!(None, out.get(3));
    }

    #[test]
    fn select_select_repeat() {
        let orig = SelectionVector::from_iter([4, 5, 6, 7]);
        let selection = SelectionVector::from_iter([1, 1, 2, 2, 2]);

        let out = orig.select(&selection);

        assert_eq!(Some(5), out.get(0));
        assert_eq!(Some(5), out.get(1));
        assert_eq!(Some(6), out.get(2));
        assert_eq!(Some(6), out.get(3));
        assert_eq!(Some(6), out.get(4));
        assert_eq!(None, out.get(5));
    }
}
