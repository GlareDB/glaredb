use rayexec_error::Result;

/// Similar to `IntoIterator`, but for an iterator with an exact size.
pub trait IntoExactSizedIterator {
    type Item;
    type IntoIter: ExactSizeIterator<Item = Self::Item>;

    /// Converts self into the `ExactSizeIteror`.
    fn into_iter(self) -> Self::IntoIter;
}

impl<I> IntoExactSizedIterator for I
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

pub trait FromExactSizedIterator<A>: Sized {
    fn from_iter<T: IntoExactSizedIterator<Item = A>>(iter: T) -> Self;
}
