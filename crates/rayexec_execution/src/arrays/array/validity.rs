use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::bitmap::Bitmap;

/// Validity mask for an array.
// TODO: Remove PartialEq
#[derive(Debug, Clone)]
pub struct Validity {
    inner: ValidityInner,
}

#[derive(Debug, Clone, PartialEq)]
enum ValidityInner {
    /// No mask has been set, assume all entries valid.
    AllValid { len: usize },
    /// All entries invalid.
    AllInvalid { len: usize },
    /// Mask has been set. Bitmap indicates which entries are valid or invalid.
    Mask { bitmap: Bitmap },
}

impl Validity {
    pub fn new_all_valid(len: usize) -> Self {
        Validity {
            inner: ValidityInner::AllValid { len },
        }
    }

    pub fn new_all_invalid(len: usize) -> Self {
        Validity {
            inner: ValidityInner::AllInvalid { len },
        }
    }

    pub fn len(&self) -> usize {
        match &self.inner {
            ValidityInner::AllValid { len } => *len,
            ValidityInner::AllInvalid { len } => *len,
            ValidityInner::Mask { bitmap } => bitmap.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all_valid(&self) -> bool {
        match &self.inner {
            ValidityInner::AllValid { .. } => true,
            ValidityInner::AllInvalid { .. } => false,
            ValidityInner::Mask { bitmap } => bitmap.is_all_true(),
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match &self.inner {
            ValidityInner::AllValid { .. } => true,
            ValidityInner::AllInvalid { .. } => false,
            ValidityInner::Mask { bitmap } => bitmap.value(idx),
        }
    }

    pub fn set_valid(&mut self, idx: usize) {
        match &mut self.inner {
            ValidityInner::AllValid { .. } => (), // Already valid,
            ValidityInner::AllInvalid { len } => {
                let mut bitmap = Bitmap::new_with_all_false(*len);
                bitmap.set_unchecked(idx, true);
                self.inner = ValidityInner::Mask { bitmap }
            }
            ValidityInner::Mask { bitmap } => bitmap.set_unchecked(idx, true),
        }
    }

    pub fn set_invalid(&mut self, idx: usize) {
        match &mut self.inner {
            ValidityInner::AllValid { len } => {
                let mut bitmap = Bitmap::new_with_all_true(*len);
                bitmap.set_unchecked(idx, false);
                self.inner = ValidityInner::Mask { bitmap }
            }
            ValidityInner::AllInvalid { .. } => (), // Nothing to do, already invalid.
            ValidityInner::Mask { bitmap } => bitmap.set_unchecked(idx, false),
        }
    }

    pub fn iter(&self) -> ValidityIter {
        ValidityIter {
            idx: 0,
            validity: self,
        }
    }

    /// Produce a new validity bitmap by applying a selection on an existing
    /// mask.
    pub fn select(&self, selection: impl IntoExactSizeIterator<Item = usize>) -> Self {
        let selection = selection.into_exact_size_iter();
        match &self.inner {
            ValidityInner::AllValid { .. } => Self::new_all_valid(selection.len()),
            ValidityInner::AllInvalid { .. } => Self::new_all_invalid(selection.len()),
            ValidityInner::Mask { bitmap } => {
                let new_mask: Bitmap = selection.map(|sel_idx| bitmap.value(sel_idx)).collect();
                Validity {
                    inner: ValidityInner::Mask { bitmap: new_mask },
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ValidityIter<'a> {
    idx: usize,
    validity: &'a Validity,
}

impl Iterator for ValidityIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.validity.len() {
            return None;
        }

        let val = self.validity.is_valid(self.idx);
        self.idx += 1;
        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_all_valid() {
        let v = Validity::new_all_valid(3);
        let new_v = v.select([1, 2, 0]);

        assert!(new_v.all_valid());
    }

    #[test]
    fn select_some_valid() {
        let mut v = Validity::new_all_valid(3);
        v.set_invalid(1);
        let new_v = v.select([1, 2, 0]);

        assert!(!new_v.is_valid(0));
        assert!(new_v.is_valid(1));
        assert!(new_v.is_valid(2));
    }
}
