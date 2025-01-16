#[derive(Debug, Clone, Copy)]
pub enum Selection<'a> {
    /// Constant selection.
    ///
    /// All indices point to the same location.
    Constant { len: usize, loc: usize },
    /// Represents a linear selection starting at some index.
    ///
    /// 'start..(start + len)'
    Linear { start: usize, len: usize },
    /// Slice of indices that indicate rows that are selected.
    ///
    /// Row indices may be included more than once and be in any order.
    Slice(&'a [usize]),
}

impl<'a> Selection<'a> {
    pub fn constant(len: usize, loc: usize) -> Self {
        Self::Constant { len, loc }
    }

    pub fn linear(start: usize, len: usize) -> Self {
        Self::Linear { start, len }
    }

    pub fn slice(sel: &'a [usize]) -> Self {
        Self::Slice(sel)
    }

    pub fn is_linear(&self) -> bool {
        matches!(self, Selection::Linear { .. })
    }

    pub fn iter(&self) -> FlatSelectionIter {
        FlatSelectionIter { idx: 0, sel: *self }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Constant { len, .. } => *len,
            Self::Linear { len, .. } => *len,
            Self::Slice(sel) => sel.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<usize> {
        match self {
            Self::Constant { len, loc } => {
                if idx >= *len {
                    None
                } else {
                    Some(*loc)
                }
            }
            Self::Linear { start, len } => {
                if idx >= *len {
                    None
                } else {
                    Some(idx + start)
                }
            }
            Self::Slice(sel) => sel.get(idx).copied(),
        }
    }
}

impl<'a> IntoIterator for Selection<'a> {
    type Item = usize;
    type IntoIter = FlatSelectionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FlatSelectionIter { idx: 0, sel: self }
    }
}

#[derive(Debug)]
pub struct FlatSelectionIter<'a> {
    idx: usize,
    sel: Selection<'a>,
}

impl Iterator for FlatSelectionIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.sel.len() {
            return None;
        }

        let v = match self.sel {
            Selection::Constant { loc, .. } => loc,
            Selection::Linear { start, .. } => self.idx + start,
            Selection::Slice(sel) => sel[self.idx],
        };

        self.idx += 1;

        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.sel.len() - self.idx;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for FlatSelectionIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_constant() {
        let sel = Selection::constant(4, 2);

        assert_eq!(Some(2), sel.get(0));
        assert_eq!(Some(2), sel.get(1));
        assert_eq!(Some(2), sel.get(2));
        assert_eq!(Some(2), sel.get(3));
        assert_eq!(None, sel.get(4));
    }

    #[test]
    fn select_linear_with_offset() {
        let sel = Selection::linear(2, 3);

        assert_eq!(Some(2), sel.get(0));
        assert_eq!(Some(3), sel.get(1));
        assert_eq!(Some(4), sel.get(2));
        assert_eq!(None, sel.get(3));
    }

    #[test]
    fn select_linear_with_offset_iter() {
        let sel = Selection::linear(2, 3);

        let out: Vec<_> = sel.into_iter().collect();
        assert_eq!(vec![2, 3, 4], out);
    }

    #[test]
    fn select_slice() {
        let sel = Selection::slice(&[4, 1, 2]);

        assert_eq!(Some(4), sel.get(0));
        assert_eq!(Some(1), sel.get(1));
        assert_eq!(Some(2), sel.get(2));
        assert_eq!(None, sel.get(3));
    }
}
