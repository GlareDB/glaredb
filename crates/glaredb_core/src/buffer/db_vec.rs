use std::mem::MaybeUninit;
use std::ptr::NonNull;

use glaredb_error::Result;

use super::buffer_manager::{AsRawBufferManager, RawBufferManager, Reservation};

#[derive(Debug)]
pub struct DbVec<T> {
    raw: RawDbVec<T>,
    len: usize,
}

impl<T> DbVec<T> {
    pub fn new_uninit(manager: &impl AsRawBufferManager, len: usize) -> Result<Self> {
        unimplemented!()
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[T] {
        unimplemented!()
    }

    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unimplemented!()
    }
}

#[derive(Debug)]
pub(crate) struct RawDbVec<T> {
    pub(crate) manager: RawBufferManager,
    /// Memory reservation for this buffer.
    ///
    /// Stores the size in bytes of this buffer.
    pub(crate) reservation: Reservation,
    /// Raw pointer to start of vec.
    pub(crate) ptr: NonNull<MaybeUninit<T>>,
    /// Capacity for the number of elements this buffer can
    /// hold.
    ///
    /// This is needed in additional to the reservation to properly handle
    /// zero-sized types (untyped null).
    pub(crate) capacity: usize,
}

unsafe impl<T> Send for RawDbVec<T> where T: Send {}
unsafe impl<T> Sync for RawDbVec<T> where T: Sync {}

impl<T> RawDbVec<T> {
    pub fn new_uninit(manager: &impl AsRawBufferManager, len: usize) -> Result<Self> {
        unimplemented!()
    }
}
