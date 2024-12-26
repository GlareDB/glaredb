use std::fmt::Debug;

/// In-memory array storage that can be directly indexed into.
pub trait AddressableStorage: Debug {
    /// The type we can get from the storage.
    type T: Send + Debug + ?Sized;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, idx: usize) -> Option<&Self::T>;
}

impl<T> AddressableStorage for &[T]
where
    T: Debug + Send,
{
    type T = T;

    fn len(&self) -> usize {
        (**self).len()
    }

    fn get(&self, idx: usize) -> Option<&Self::T> {
        (**self).get(idx)
    }
}

impl<T> AddressableStorage for &mut [T]
where
    T: Debug + Send + Copy,
{
    type T = T;

    fn len(&self) -> usize {
        (**self).len()
    }

    fn get(&self, idx: usize) -> Option<&Self::T> {
        (**self).get(idx)
    }
}

pub trait MutableAddressableStorage: AddressableStorage {
    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T>;

    fn put(&mut self, idx: usize, val: &Self::T);
}

impl<T> MutableAddressableStorage for &mut [T]
where
    T: Debug + Send + Copy,
{
    fn get_mut(&mut self, idx: usize) -> Option<&mut Self::T> {
        (**self).get_mut(idx)
    }

    fn put(&mut self, idx: usize, val: &Self::T) {
        self[idx] = *val;
    }
}
