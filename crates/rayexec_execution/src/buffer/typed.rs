use std::ops::{Deref, DerefMut};

use rayexec_error::Result;
use stdutil::marker::PhantomCovariant;

use super::buffer_manager::BufferManager;
use super::raw::RawBuffer;

/// Type alias to a raw buffer storing bytes.
pub type ByteBuffer<B> = TypedBuffer<u8, B>;

/// Wrapper around a raw buffer that knows its type.
#[derive(Debug)]
pub struct TypedBuffer<T, B: BufferManager> {
    pub(crate) _type: PhantomCovariant<T>,
    pub(crate) raw: RawBuffer<B>,
}

impl<T, B> TypedBuffer<T, B>
where
    B: BufferManager,
{
    pub fn empty(manager: &B) -> Self {
        let raw = RawBuffer::try_with_capacity::<T>(manager, 0)
            .expect("allocating zero sized buffer to no fail");
        TypedBuffer {
            _type: PhantomCovariant::new(),
            raw,
        }
    }

    /// Create a new buffer that can hold `cap` number of entries.
    pub fn try_with_capacity(manager: &B, cap: usize) -> Result<Self> {
        let raw = RawBuffer::try_with_capacity::<T>(manager, cap)?;
        Ok(TypedBuffer {
            _type: PhantomCovariant::new(),
            raw,
        })
    }

    /// Resizes the buffer if the current capacity is less than `size` in number
    /// of `T` elements.
    ///
    /// Does nothing if the current capacity is sufficient.
    ///
    /// Attempts to amortize reallocations by doubling the current capacity if
    /// sufficient.
    pub fn reserve_for_size(&mut self, size: usize) -> Result<()> {
        if self.raw.typed_capacity() < size {
            let new_cap = usize::max(size, self.raw.typed_capacity() * 2);
            let additional = new_cap - self.raw.typed_capacity();
            self.reserve_additional(additional)?;
        }

        Ok(())
    }

    /// Resize this buffer to hold exactly `additional` number of entries.
    pub fn reserve_additional(&mut self, additional: usize) -> Result<()> {
        unsafe { self.raw.reserve::<T>(additional) }
    }

    /// Returns the capacity of this buffer.
    pub const fn capacity(&self) -> usize {
        self.raw.typed_capacity()
    }

    pub fn as_ptr(&self) -> *const T {
        self.raw.as_ptr().cast()
    }

    pub fn as_mut_ptr(&self) -> *mut T {
        self.raw.as_mut_ptr().cast()
    }

    /// Convert this buffer to a slice.
    pub fn as_slice(&self) -> &[T] {
        unsafe { self.raw.as_slice::<T>() }
    }

    /// Convert this buffer to a mutable slice.
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { self.raw.as_slice_mut() }
    }
}

impl<T, B> AsRef<[T]> for TypedBuffer<T, B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T, B> AsMut<[T]> for TypedBuffer<T, B>
where
    B: BufferManager,
{
    fn as_mut(&mut self) -> &mut [T] {
        self.as_slice_mut()
    }
}

/// Wrapper around a typed raw buffer that has a manual alignemnt.
#[derive(Debug)]
pub struct AlignedBuffer<T, B: BufferManager>(TypedBuffer<T, B>);

impl<T, B> AlignedBuffer<T, B>
where
    B: BufferManager,
{
    pub fn try_with_capacity_and_alignment(manager: &B, cap: usize, align: usize) -> Result<Self> {
        let raw = RawBuffer::try_with_capacity_and_alignment::<T>(manager, cap, align)?;
        Ok(AlignedBuffer(TypedBuffer {
            _type: PhantomCovariant::new(),
            raw,
        }))
    }

    /// Gets the underlying typed buffer.
    ///
    /// The returned buffer retains the custom alignment.
    pub fn into_typed_raw_buffer(self) -> TypedBuffer<T, B> {
        self.0
    }
}

impl<T, B> AsRef<TypedBuffer<T, B>> for AlignedBuffer<T, B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &TypedBuffer<T, B> {
        &self.0
    }
}

impl<T, B> AsMut<TypedBuffer<T, B>> for AlignedBuffer<T, B>
where
    B: BufferManager,
{
    fn as_mut(&mut self) -> &mut TypedBuffer<T, B> {
        &mut self.0
    }
}

impl<T, B> Deref for AlignedBuffer<T, B>
where
    B: BufferManager,
{
    type Target = TypedBuffer<T, B>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, B> DerefMut for AlignedBuffer<T, B>
where
    B: BufferManager,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;

    #[test]
    fn reserve_for_size_no_increase() {
        let mut buf = ByteBuffer::try_with_capacity(&NopBufferManager, 14).unwrap();
        buf.reserve_for_size(12).unwrap();
        assert_eq!(14, buf.capacity());
    }

    #[test]
    fn reserve_for_size_with_increase() {
        let mut buf = ByteBuffer::try_with_capacity(&NopBufferManager, 14).unwrap();
        buf.reserve_for_size(16).unwrap();
        assert!(buf.capacity() >= 16);
    }
}
