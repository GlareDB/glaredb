use std::fmt;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EntryKey<T> {
    pub hash: u64,
    pub key: T,
}

impl<T> EntryKey<T>
where
    T: fmt::Debug + Default + Clone + Copy + PartialEq + Eq,
{
    pub const fn new(hash: u64, key: T) -> Self {
        EntryKey { hash, key }
    }

    pub const fn is_empty(&self) -> bool {
        self.hash == 0
    }
}
