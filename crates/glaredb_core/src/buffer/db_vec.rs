use std::mem::MaybeUninit;
use std::ptr::NonNull;

use glaredb_error::Result;

use super::buffer_manager::{AsRawBufferManager, RawBufferManager, Reservation};
use crate::util::marker::PhantomCovariant;

/// A `Vec` type that's backed by a buffer manager.
///
/// This should not be used as a general purpose replacement for
/// `std::vec::Vec`. This is meant for allocations that last the life time of a
/// query.
///
/// Memory is left unitialized after allocating. Care needs to be taken to
/// ensure the vec is written to before it's read from.
#[derive(Debug)]
pub struct DbVec<T> {
    raw: RawDbVec<T>,
    len: usize,
}

impl<T> DbVec<T> {
    /// Create a new vec backed by the given manager with an initial of _at
    /// least_ `len`.
    ///
    /// This will allocate memory needed, however that memory will remain
    /// unitilialized. It's important that a value is written to a given index
    /// before it's read.
    pub fn new_uninit(manager: &impl AsRawBufferManager, len: usize) -> Result<Self> {
        let raw = RawDbVec::new_uninit(manager, len)?;
        Ok(DbVec { raw, len })
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn capacity(&self) -> usize {
        self.raw.capacity()
    }

    /// Get a slice reference for the elements in this vec.
    ///
    /// # Safety
    ///
    /// - Reading an element at a given index requires that it's been written to
    ///   previously.
    /// - There should exist no mutable references to anything within this vec's
    ///   allocation (by creating it from a raw pointer).
    pub unsafe fn as_slice(&self) -> &[T] {
        debug_assert!(self.len() <= self.capacity());
        let ptr = self.raw.ptr().cast().as_ptr();
        unsafe { std::slice::from_raw_parts(ptr, self.len) }
    }

    /// Get a mutable slice reference for elements in the vec.
    ///
    /// This can be used to initialize the memory.
    ///
    /// # Safety
    ///
    /// - Reading an element at a given index requires that it's been written to
    ///   previously.
    /// - There should exist no mutable or immutable references to anything
    ///   within this vec's allocation (by creating it from a raw pointer).
    pub unsafe fn as_slice_mut(&mut self) -> &mut [T] {
        debug_assert!(self.len() <= self.capacity());
        let ptr = self.raw.ptr().cast().as_ptr();
        unsafe { std::slice::from_raw_parts_mut(ptr, self.len) }
    }
}

impl<T> Drop for DbVec<T> {
    fn drop(&mut self) {
        unsafe {
            // SAFETY: If we have a dangling reference somewhere, we already
            // lost.
            let slice = self.as_slice_mut();
            std::ptr::drop_in_place(slice);
        }

        // `raw` implicitly dropped...
    }
}

#[derive(Debug)]
pub(crate) struct RawDbVec<T> {
    manager: RawBufferManager,
    /// Memory reservation for this buffer.
    ///
    /// Stores the size in bytes of this buffer.
    reservation: Reservation,
    /// Capacity for the number of elements this buffer can
    /// hold.
    ///
    /// This is needed in additional to the reservation to properly handle
    /// zero-sized types (untyped null).
    capacity: usize,
    _t: PhantomCovariant<T>,
}

unsafe impl<T> Send for RawDbVec<T> where T: Send {}
unsafe impl<T> Sync for RawDbVec<T> where T: Sync {}

impl<T> RawDbVec<T> {
    pub fn new_uninit(manager: &impl AsRawBufferManager, cap: usize) -> Result<Self> {
        let manager = manager.as_raw_buffer_manager();

        let size = std::mem::size_of::<T>() * cap;
        let align = std::mem::align_of::<T>();
        let reservation = unsafe { manager.call_reserve(size, align) }?;

        let capacity = if size == 0 {
            // Zero-sized type, we can fit up to usize::MAX elements.
            usize::MAX
        } else {
            reservation.size() / std::mem::size_of::<T>()
        };

        Ok(RawDbVec {
            manager,
            reservation,
            capacity,
            _t: PhantomCovariant::new(),
        })
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn ptr(&self) -> NonNull<MaybeUninit<T>> {
        self.reservation.ptr().cast()
    }

    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        unsafe { self.manager.call_resize(&mut self.reservation, new_size)? };
        self.capacity = self.reservation.size() / std::mem::size_of::<T>();
        Ok(())
    }
}

impl<T> Drop for RawDbVec<T> {
    fn drop(&mut self) {
        unsafe { self.manager.call_free_reservation(&mut self.reservation) };
    }
}
