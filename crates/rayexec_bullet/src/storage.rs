use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::Arc;

/// Marker trait for a deallocation mechanism for the `PrimitiveStorage::Raw`
/// variant.
///
/// The implementation of `Drop` should handle deallocating the data.
pub trait RawDeallocate: Send + Sync + fmt::Debug {}

#[allow(dead_code)]
#[derive(Debug)]
struct VecDeallocate {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl Drop for VecDeallocate {
    fn drop(&mut self) {
        let v = unsafe { Vec::from_raw_parts(self.ptr, self.len, self.cap) };
        std::mem::drop(v)
    }
}

/// Backing storage for primitive values.
///
/// Currently this contains only a single variant, but should be extension point
/// for working with externally managed data (Arrow arrays from arrow-rs, shared
/// memory regions, CUDA, etc).
#[derive(Debug)]
pub enum PrimitiveStorage<T> {
    /// A basic vector of data.
    Vec(Vec<T>),

    /// Pointer to a raw slice of data that's potentially been externally
    /// allocated.
    // TODO: Don't use, just thinking about ffi.
    Raw {
        ptr: *const T,
        len: usize,
        deallocate: Arc<dyn RawDeallocate>,
    },
}

unsafe impl<T: Send> Send for PrimitiveStorage<T> {}
unsafe impl<T: Sync> Sync for PrimitiveStorage<T> {}

impl<T> PrimitiveStorage<T> {
    /// A potentially failable conversion to a mutable slice reference.
    ///
    /// This will only succeed for the Vec variant.
    pub fn try_as_mut(&mut self) -> Result<&mut [T]> {
        match self {
            Self::Vec(v) => Ok(v),
            Self::Raw { .. } => Err(RayexecError::new(
                "Cannot get a mutable reference to raw value storage",
            )),
        }
    }

    /// Copies a bytes slice into a newly allocated primitive storage for the
    /// primitive type.
    ///
    /// Assumes that the bytes provided represents a valid contiguous slice of
    /// `T`.
    pub fn copy_from_bytes(bytes: &[u8]) -> Result<Self>
    where
        T: Default + Copy,
    {
        if bytes.len() % std::mem::size_of::<T>() != 0 {
            return Err(RayexecError::new(format!(
                "Byte slice is not valid for type, bytes len: {}",
                bytes.len(),
            )));
        }

        let cap = bytes.len() / std::mem::size_of::<T>();

        let vec: Vec<T> = vec![T::default(); cap];
        let mut manual = ManuallyDrop::new(vec);
        let ptr = manual.as_mut_ptr();
        let len = manual.len();
        let cap = manual.capacity();

        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.cast(), bytes.len()) };

        let vec = unsafe { Vec::from_raw_parts(ptr, len, cap) };

        Ok(PrimitiveStorage::Vec(vec))
    }

    /// Returns self as a slice of bytes.
    ///
    /// Represents a contiguous slice of `T` as bytes (native endian).
    pub fn as_bytes(&self) -> &[u8] {
        let s = self.as_ref();
        let ptr = s.as_ptr();
        // Suggested by clippy instead of manually computing the size.
        //
        // See: <https://rust-lang.github.io/rust-clippy/master/index.html#/manual_slice_size_calculation>
        let num_bytes = std::mem::size_of_val(s);

        unsafe { std::slice::from_raw_parts(ptr.cast(), num_bytes) }
    }
}

/// Implementation of equality that compares the actual values regardless of if
/// they're stored in a vector or using a raw pointer.
impl<T: PartialEq> PartialEq for PrimitiveStorage<T> {
    fn eq(&self, other: &Self) -> bool {
        let a = self.as_ref();
        let b = other.as_ref();
        a == b
    }
}

impl<T> From<Vec<T>> for PrimitiveStorage<T> {
    fn from(value: Vec<T>) -> Self {
        PrimitiveStorage::Vec(value)
    }
}

impl<T> AsRef<[T]> for PrimitiveStorage<T> {
    fn as_ref(&self) -> &[T] {
        match self {
            Self::Vec(v) => v.as_slice(),
            Self::Raw { ptr, len, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Assumes little endian.

    #[test]
    fn from_bytes() {
        let bs = [1, 0, 0, 0, 2, 0, 0, 0];
        let primitive = PrimitiveStorage::<u32>::copy_from_bytes(&bs).unwrap();

        assert_eq!(&[1, 2], primitive.as_ref())
    }

    #[test]
    fn from_bytes_invalid_len() {
        let bs = [1, 0, 0, 0, 2, 0, 0, 0, 3];
        let _ = PrimitiveStorage::<u32>::copy_from_bytes(&bs).unwrap_err();
    }

    #[test]
    fn as_bytes() {
        let s = PrimitiveStorage::<u32>::from(vec![1, 2]);
        assert_eq!(&[1, 0, 0, 0, 2, 0, 0, 0], s.as_bytes())
    }

    #[test]
    fn bytes_roundtrip() {
        let s1 = PrimitiveStorage::<u32>::from(vec![8, 9, 10]);
        let s2 = PrimitiveStorage::<u32>::copy_from_bytes(s1.as_bytes()).unwrap();

        assert_eq!(s1, s2);
    }
}
