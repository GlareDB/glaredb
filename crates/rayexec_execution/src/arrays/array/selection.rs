#[derive(Debug, Clone, Copy)]
pub enum Selection<'a> {
    /// Constant selection.
    ///
    /// All indices point to the same location.
    Constant { len: usize, loc: usize },
    /// Represents a linear selection.
    ///
    /// '0..len'
    Linear { len: usize },
    /// Represents the true location to use for some index.
    Selection(&'a [usize]),
}

impl<'a> Selection<'a> {
    pub fn constant(len: usize, loc: usize) -> Self {
        Self::Constant { len, loc }
    }

    pub fn linear(len: usize) -> Self {
        Self::Linear { len }
    }

    pub fn selection(sel: &'a [usize]) -> Self {
        Self::Selection(sel)
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
            Self::Linear { len } => *len,
            Self::Selection(sel) => sel.len(),
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
            Self::Linear { len } => {
                if idx >= *len {
                    None
                } else {
                    Some(idx)
                }
            }
            Self::Selection(sel) => sel.get(idx).copied(),
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
            Selection::Linear { .. } => self.idx,
            Selection::Selection(sel) => sel[self.idx],
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
