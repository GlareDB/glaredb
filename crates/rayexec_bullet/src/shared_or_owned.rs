use std::sync::Arc;

/// Wrapper around either a shared (Arc) or owned `T`.
///
/// This type allows for easily converting to and from shared variants of some
/// underlying type (e.g. selection vectors or bitmaps).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedOrOwned<T> {
    inner: SharedOrOwnedInner<T>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SharedOrOwnedInner<T> {
    Shared(Arc<T>),
    Owned(T),
    Empty,
}

impl<T> From<T> for SharedOrOwned<T> {
    fn from(value: T) -> Self {
        SharedOrOwned {
            inner: SharedOrOwnedInner::Owned(value),
        }
    }
}

impl<T> From<Arc<T>> for SharedOrOwned<T> {
    fn from(value: Arc<T>) -> Self {
        SharedOrOwned {
            inner: SharedOrOwnedInner::Shared(value),
        }
    }
}

impl<T> AsRef<T> for SharedOrOwned<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        match &self.inner {
            SharedOrOwnedInner::Shared(v) => v.as_ref(),
            SharedOrOwnedInner::Owned(v) => v,
            SharedOrOwnedInner::Empty => unreachable!("invalid state"),
        }
    }
}

impl<T> SharedOrOwned<T>
where
    T: Default + Clone,
{
    /// Converts to an Owned variant of self.
    ///
    /// Does nothing if already Owned.
    ///
    /// This may clone the underlying type if there's more than one reference.
    pub fn make_owned(&mut self) {
        if self.is_shared() {
            let inner = std::mem::replace(&mut self.inner, SharedOrOwnedInner::Empty);
            let v = match inner {
                SharedOrOwnedInner::Shared(v) => Arc::unwrap_or_clone(v),
                _ => panic!("invalid state"),
            };

            self.inner = SharedOrOwnedInner::Owned(v)
        }
    }

    /// Converts to a shared variant of self.
    pub fn make_shared(&mut self) {
        if self.is_owned() {
            let inner = std::mem::replace(&mut self.inner, SharedOrOwnedInner::Empty);
            let v = match inner {
                SharedOrOwnedInner::Owned(v) => Arc::new(v),
                _ => panic!("invalid state"),
            };

            self.inner = SharedOrOwnedInner::Shared(v)
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.make_owned();
        match &mut self.inner {
            SharedOrOwnedInner::Owned(v) => v,
            _ => panic!("invalid state"),
        }
    }

    pub fn is_shared(&self) -> bool {
        matches!(self.inner, SharedOrOwnedInner::Shared(_))
    }

    pub fn is_owned(&self) -> bool {
        matches!(self.inner, SharedOrOwnedInner::Owned(_))
    }
}
