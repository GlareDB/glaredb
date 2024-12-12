use super::AddressableStorage;

/// Unit value representing untyped null values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UntypedNull;

/// Storage for untyped null values.
///
/// Doesn't actually store anything other than the length of data being
/// represented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UntypedNullStorage(pub(crate) usize);

impl AddressableStorage for UntypedNullStorage {
    type T = UntypedNull;

    fn len(&self) -> usize {
        self.0
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        if idx >= self.0 {
            return None;
        }
        Some(UntypedNull)
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        self.get(idx).unwrap()
    }
}
