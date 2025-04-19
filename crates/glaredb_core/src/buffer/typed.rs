use std::ops::{Deref, DerefMut};

use glaredb_error::Result;

use super::buffer_manager::AsRawBufferManager;
use super::raw::RawBuffer;
use crate::util::marker::PhantomCovariant;

/// Type alias to a raw buffer storing bytes.
pub type ByteBuffer = TypedBuffer<u8>;

/// Wrapper around a raw buffer that knows its type.
#[derive(Debug)]
pub struct TypedBuffer<T> {
    pub(crate) _type: PhantomCovariant<T>,
    pub(crate) raw: RawBuffer,
}

impl<T> TypedBuffer<T> {
    pub fn empty(manager: &impl AsRawBufferManager) -> Self {
        let raw = RawBuffer::try_with_capacity::<T>(manager, 0)
            .expect("allocating zero sized buffer to no fail");
        TypedBuffer {
            _type: PhantomCovariant::new(),
            raw,
        }
    }

    /// Create a new buffer that can hold `cap` number of entries.
    pub fn try_with_capacity(manager: &impl AsRawBufferManager, cap: usize) -> Result<Self> {
        let raw = RawBuffer::try_with_capacity::<T>(manager, cap)?;
        Ok(TypedBuffer {
            _type: PhantomCovariant::new(),
            raw,
        })
    }

    /// Resizes the buffer if the current capacity is less than `size` in number
    /// of `T` elements.
    ///
    /// Returns a bool indicating if the buffer did reallocate.
    ///
    /// Attempts to amortize reallocations by doubling the current capacity if
    /// sufficient.
    pub fn reserve_for_size(&mut self, size: usize) -> Result<bool> {
        if self.raw.typed_capacity() < size {
            let new_cap = usize::max(size, self.raw.typed_capacity() * 2);
            let additional = new_cap - self.raw.typed_capacity();
            self.reserve_additional(additional)?;

            return Ok(true);
        }

        Ok(false)
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

impl<T> AsRef<[T]> for TypedBuffer<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> AsMut<[T]> for TypedBuffer<T> {
    fn as_mut(&mut self) -> &mut [T] {
        self.as_slice_mut()
    }
}

/// Wrapper around a typed raw buffer that has a manual alignemnt.
#[derive(Debug)]
pub struct AlignedBuffer<T>(TypedBuffer<T>);

impl<T> AlignedBuffer<T> {
    pub fn try_with_capacity_and_alignment(
        manager: &impl AsRawBufferManager,
        cap: usize,
        align: usize,
    ) -> Result<Self> {
        let raw = RawBuffer::try_with_capacity_and_alignment::<T>(manager, cap, align)?;
        Ok(AlignedBuffer(TypedBuffer {
            _type: PhantomCovariant::new(),
            raw,
        }))
    }

    /// Gets the underlying typed buffer.
    ///
    /// The returned buffer retains the custom alignment.
    pub fn into_typed_raw_buffer(self) -> TypedBuffer<T> {
        self.0
    }
}

impl<T> AsRef<TypedBuffer<T>> for AlignedBuffer<T> {
    fn as_ref(&self) -> &TypedBuffer<T> {
        &self.0
    }
}

impl<T> AsMut<TypedBuffer<T>> for AlignedBuffer<T> {
    fn as_mut(&mut self) -> &mut TypedBuffer<T> {
        &mut self.0
    }
}

impl<T> Deref for AlignedBuffer<T> {
    type Target = TypedBuffer<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for AlignedBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;

    #[test]
    fn reserve_for_size_no_increase() {
        let mut buf = ByteBuffer::try_with_capacity(&DefaultBufferManager, 14).unwrap();
        buf.reserve_for_size(12).unwrap();
        assert_eq!(14, buf.capacity());
    }

    #[test]
    fn reserve_for_size_with_increase() {
        let mut buf = ByteBuffer::try_with_capacity(&DefaultBufferManager, 14).unwrap();
        buf.reserve_for_size(16).unwrap();
        assert!(buf.capacity() >= 16);
    }
}
