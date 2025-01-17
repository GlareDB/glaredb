/// Similar to `IntoIterator`, but for an iterator with an exact size.
#[deprecated]
pub trait IntoExtactSizeIterator {
    type Item;
    type IntoIter: ExactSizeIterator<Item = Self::Item>;

    /// Converts self into the `ExactSizeIteror`.
    fn into_iter(self) -> Self::IntoIter;
}
