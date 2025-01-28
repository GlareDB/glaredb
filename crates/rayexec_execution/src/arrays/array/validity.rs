use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::bitmap::view::{num_bytes_for_bitmap, BitmapView, BitmapViewMut};

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
    Mask { len: usize, data: Vec<u8> },
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
            ValidityInner::Mask { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all_valid(&self) -> bool {
        match &self.inner {
            ValidityInner::AllValid { .. } => true,
            ValidityInner::AllInvalid { .. } => false,
            ValidityInner::Mask { len, data } => BitmapView::new(data, *len).all_true(),
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match &self.inner {
            ValidityInner::AllValid { .. } => true,
            ValidityInner::AllInvalid { .. } => false,
            ValidityInner::Mask { len, data } => BitmapView::new(data, *len).value(idx),
        }
    }

    pub fn set_valid(&mut self, idx: usize) {
        match &mut self.inner {
            ValidityInner::AllValid { .. } => (), // Already valid,
            ValidityInner::AllInvalid { len } => {
                let mut data = vec![0; num_bytes_for_bitmap(*len)];
                BitmapViewMut::new(&mut data, *len).set(idx);
                self.inner = ValidityInner::Mask { data, len: *len }
            }
            ValidityInner::Mask { len, data } => BitmapViewMut::new(data, *len).set(idx),
        }
    }

    pub fn set_invalid(&mut self, idx: usize) {
        match &mut self.inner {
            ValidityInner::AllValid { len } => {
                let mut data = vec![u8::MAX; num_bytes_for_bitmap(*len)];
                BitmapViewMut::new(&mut data, *len).unset(idx);
                self.inner = ValidityInner::Mask { data, len: *len }
            }
            ValidityInner::AllInvalid { .. } => (), // Nothing to do, already invalid.
            ValidityInner::Mask { len, data } => BitmapViewMut::new(data, *len).unset(idx),
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
            ValidityInner::Mask { len, data } => {
                let new_len = selection.len();
                let mut new_data = vec![0; num_bytes_for_bitmap(new_len)];
                let mut new_view = BitmapViewMut::new(&mut new_data, new_len);
                let old_view = BitmapView::new(data, *len);

                for (out_idx, sel_idx) in selection.enumerate() {
                    // Initialized to all false, just set if true.
                    if old_view.value(sel_idx) {
                        new_view.set(out_idx);
                    }
                }

                Validity {
                    inner: ValidityInner::Mask {
                        len: new_len,
                        data: new_data,
                    },
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
