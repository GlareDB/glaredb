use rayexec_error::{RayexecError, Result};
use std::fmt;

/// Marker trait for a deallocation mechanism for the `PrimitiveStorage::Raw`
/// variant.
///
/// The implementation of `Drop` should handle deallocating the data.
pub trait RawDeallocate: Send + Sync + fmt::Debug {}

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
        deallocate: Box<dyn RawDeallocate>,
    },
}

unsafe impl<T: Send> Send for PrimitiveStorage<T> {}
unsafe impl<T: Sync> Sync for PrimitiveStorage<T> {}

impl<T> PrimitiveStorage<T> {
    /// A potentially failable conversion to a mutable slice reference.
    pub fn try_as_mut(&mut self) -> Result<&mut [T]> {
        match self {
            Self::Vec(v) => Ok(v),
            Self::Raw { .. } => Err(RayexecError::new(
                "Cannot get a mutable reference to raw value storage",
            )),
        }
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
