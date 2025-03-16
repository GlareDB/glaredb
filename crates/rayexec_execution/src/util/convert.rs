/// Try to do a cheap mutable-to-mutable reference conversion.
pub trait TryAsMut<T>
where
    T: ?Sized,
{
    /// Error returned if the conversion fails.
    type Error;

    fn try_as_mut(&mut self) -> Result<&mut T, Self::Error>;
}
