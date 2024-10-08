use super::AddressableStorage;
use crate::bitmap::Bitmap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanStorage(pub(crate) Bitmap);

impl BooleanStorage {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_boolean_storage_ref(&self) -> BooleanStorageRef {
        BooleanStorageRef(&self.0)
    }
}

impl AsRef<Bitmap> for BooleanStorage {
    fn as_ref(&self) -> &Bitmap {
        &self.0
    }
}

impl From<Bitmap> for BooleanStorage {
    fn from(value: Bitmap) -> Self {
        BooleanStorage(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanStorageRef<'a>(&'a Bitmap);

impl<'a> AddressableStorage for BooleanStorageRef<'a> {
    type T = bool;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        if idx >= self.len() {
            return None;
        }

        Some(self.0.value_unchecked(idx))
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        self.0.value_unchecked(idx)
    }
}
