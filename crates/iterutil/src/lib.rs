/// Similar to `IntoIterator`, but for an iterator with an exact size.
pub trait IntoExactSizeIterator {
    type Item;
    type IntoIter: ExactSizeIterator<Item = Self::Item>;

    /// Converts self into the `ExactSizeIteror`.
    fn into_iter(self) -> Self::IntoIter;
}

/// Auto-implement for any exact size iterator.
impl<I> IntoExactSizeIterator for I
where
    I: IntoIterator,
    I::IntoIter: ExactSizeIterator,
{
    type Item = I::Item;
    type IntoIter = I::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.into_iter()
    }
}

pub trait FromExactSizeIterator<A>: Sized {
    /// Create Self from an exact size iterator.
    fn from_iter<T: IntoExactSizeIterator<Item = A>>(iter: T) -> Self;
}

pub trait TryFromExactSizeIterator<A>: Sized {
    /// Error type that will be returned.
    type Error;

    /// Try to create Self from an exact size iterator.
    fn try_from_iter<T: IntoExactSizeIterator<Item = A>>(iter: T) -> Result<Self, Self::Error>;
}
