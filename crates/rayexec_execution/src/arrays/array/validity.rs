use crate::arrays::bitmap::Bitmap;

#[derive(Debug, Clone)]
pub struct Validity {
    inner: ValidityInner,
}

#[derive(Debug, Clone)]
enum ValidityInner {
    /// No mask has been set, assume all entries valid.
    NoMask { len: usize },
    /// Mask has been set. Bitmap indicates which entries are valid or invalid.
    Mask { bitmap: Bitmap },
}

impl Validity {
    pub fn new_all_valid(len: usize) -> Self {
        Validity {
            inner: ValidityInner::NoMask { len },
        }
    }

    pub fn len(&self) -> usize {
        match &self.inner {
            ValidityInner::NoMask { len } => *len,
            ValidityInner::Mask { bitmap } => bitmap.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all_valid(&self) -> bool {
        match &self.inner {
            ValidityInner::NoMask { .. } => true,
            ValidityInner::Mask { bitmap } => bitmap.is_all_true(),
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match &self.inner {
            ValidityInner::NoMask { .. } => true,
            ValidityInner::Mask { bitmap } => bitmap.value(idx),
        }
    }

    pub fn set_valid(&mut self, idx: usize) {
        if let ValidityInner::Mask { bitmap } = &mut self.inner {
            bitmap.set_unchecked(idx, true)
        }
        // Otherwise we already assume everything is valid.
    }

    pub fn set_invalid(&mut self, idx: usize) {
        match &mut self.inner {
            ValidityInner::NoMask { len } => {
                let mut bitmap = Bitmap::new_with_all_true(*len);
                bitmap.set_unchecked(idx, false);
                self.inner = ValidityInner::Mask { bitmap }
            }
            ValidityInner::Mask { bitmap } => bitmap.set_unchecked(idx, false),
        }
    }

    pub fn iter(&self) -> ValidityIter {
        ValidityIter {
            idx: 0,
            validity: self,
        }
    }
}

#[derive(Debug)]
pub struct ValidityIter<'a> {
    idx: usize,
    validity: &'a Validity,
}

impl<'a> Iterator for ValidityIter<'a> {
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