use crate::bitmap::Bitmap;

/// A logical array for representing some number of Nulls.
#[derive(Debug, PartialEq, Eq)]
pub struct NullArray {
    validity: Bitmap,
}

impl NullArray {
    pub fn new(len: usize) -> Self {
        NullArray {
            validity: Bitmap::new_with_val(false, len),
        }
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }
        Some(false)
    }

    pub fn validity(&self) -> &Bitmap {
        &self.validity
    }
}
