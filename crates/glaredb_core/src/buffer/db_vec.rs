#![allow(clippy::len_without_is_empty)]

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use glaredb_error::{DbError, Result};

use super::buffer_manager::{AsRawBufferManager, RawBufferManager, Reservation};
use crate::util::iter::IntoExactSizeIterator;
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
    pub fn empty(manager: &impl AsRawBufferManager) -> Self {
        let raw =
            RawDbVec::new_uninit(manager, 0).expect("allocating zero sized buffer to not fail");

        DbVec { raw, len: 0 }
    }

    /// Creates a new vec with at least the given capacity.
    ///
    /// The length is initially zero.
    pub fn with_capacity(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        let raw = RawDbVec::new_uninit(manager, capacity)?;
        Ok(DbVec { raw, len: 0 })
    }

    /// Create a new vec backed by the given manager with an initial capacity of
    /// _at least_ `len`.
    ///
    /// # Safety
    ///
    /// This will allocate memory needed with the length set to that initial
    /// capacity, however that memory will remain unitilialized. It's important
    /// that a value is written to a given index before it's read.
    ///
    /// This is only allowed for `Copy` types as they cannot implement `Drop`.
    /// This provides an important guarantee that dropping this vec with any
    /// number of uninitialized elements avoids touching uninitialized memory
    /// during drop.
    pub unsafe fn new_uninit(manager: &impl AsRawBufferManager, len: usize) -> Result<Self>
    where
        T: Copy,
    {
        let raw = RawDbVec::new_uninit(manager, len)?;
        Ok(DbVec { raw, len })
    }

    pub fn new_uninit_with_align(
        manager: &impl AsRawBufferManager,
        len: usize,
        align: usize,
    ) -> Result<Self>
    where
        T: Copy,
    {
        let raw = RawDbVec::new_uninit_with_align(manager, len, align)?;
        Ok(DbVec { raw, len })
    }

    /// Create a new vec by copying from a source slice into a newly allocating
    /// vec.
    pub fn new_from_slice(manager: &impl AsRawBufferManager, slice: impl AsRef<[T]>) -> Result<Self>
    where
        T: Copy,
    {
        let src = slice.as_ref();
        let mut vec = unsafe { Self::new_uninit(manager, src.len())? };
        let dest = vec.as_slice_mut();
        dest.copy_from_slice(src);

        Ok(vec)
    }

    pub fn new_from_iter<I>(manager: &impl AsRawBufferManager, iter: I) -> Result<Self>
    where
        I: IntoExactSizeIterator<Item = T>,
        I::Item: Copy,
    {
        let iter = iter.into_exact_size_iter();
        let len = iter.len();

        let mut vec = unsafe { Self::new_uninit(manager, len)? };
        let slice = vec.as_slice_mut();

        for (src, dest) in iter.zip(slice) {
            *dest = src
        }

        Ok(vec)
    }

    /// Create a new vec, initializing all values to `val`.
    pub fn with_value(manager: &impl AsRawBufferManager, len: usize, val: T) -> Result<Self>
    where
        T: Copy,
    {
        let mut vec = unsafe { Self::new_uninit(manager, len)? };
        let s = vec.as_slice_mut();
        s.fill(val);

        Ok(vec)
    }

    pub fn with_value_init_fn<F>(
        manager: &impl AsRawBufferManager,
        len: usize,
        mut init_fn: F,
    ) -> Result<Self>
    where
        F: FnMut() -> T,
    {
        let raw = RawDbVec::new_uninit(manager, len)?;
        let mut vec = DbVec { raw, len };

        let s = unsafe { vec.as_uninit_slice_mut() };
        for uninit in s {
            uninit.write(init_fn());
        }

        Ok(vec)
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    /// Sets the length arbitrarily.
    ///
    /// Panics if the length is greater than the vecs actual capacity.
    ///
    /// # Safety
    ///
    /// This may set the length to allow accessing unitialized memory. It's
    /// important that that memory gets written to prior to reading it.
    pub unsafe fn set_len(&mut self, len: usize) {
        assert!(len <= self.raw.capacity);
        self.len = len;
    }

    pub const fn capacity(&self) -> usize {
        self.raw.capacity()
    }

    /// Pushes elements onto this vector, reserving additional capacity if
    /// needed.
    pub fn push_slice(&mut self, slice: &[T]) -> Result<()>
    where
        T: Copy,
    {
        let additional = slice.len();
        let new_len = self.len + additional;

        if new_len > self.capacity() {
            // Double strategy, but at least fit new_len.
            let cap = self.capacity();
            let double = cap.saturating_mul(2);
            let new_cap = usize::max(double, new_len);
            self.raw.resize(new_cap)?;
        }

        // Copy slice to tail.
        unsafe {
            let base = self.raw.ptr().as_ptr().cast::<T>();
            let dest = base.add(self.len);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), dest, additional);
        }

        self.len = new_len;
        Ok(())
    }

    /// Pushes elements onto this vector, erroring if this vector cannot fit the
    /// additional elements without reallocating.
    pub fn push_slice_no_resize(&mut self, slice: &[T]) -> Result<()>
    where
        T: Copy,
    {
        let additional = slice.len();
        let new_len = self.len + additional;

        if new_len > self.capacity() {
            return Err(
                DbError::new("Cannot push elements onto vector without resizing")
                    .with_field("curr_cap", self.capacity())
                    .with_field("need", new_len),
            );
        }

        // Copy slice to tail.
        unsafe {
            let base = self.raw.ptr().as_ptr().cast::<T>();
            let dest = base.add(self.len);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), dest, additional);
        }

        self.len = new_len;
        Ok(())
    }

    /// Resizes the vec to `new_len`, reallocating if needed.
    ///
    /// # Safety
    ///
    /// When reallocating, memory beyond the original length of the vec may be
    /// unitialized. It's important to write to the memory prior to reading it.
    pub unsafe fn resize_uninit(&mut self, new_len: usize) -> Result<()> {
        if new_len == self.len {
            return Ok(());
        }

        if new_len < self.len() {
            // Drop elements that are being removed.
            //
            // TODO: This is a bit sketch with non-trivial drops considering we
            // can grow the vec, then shrink it, leading to touch unitialized
            // memory. We might want to just make resize work with Copy to avoid
            // the issue.
            unsafe {
                // How many elements we're shaving off the end.
                let drop_count = self.len() - new_len;
                // Point to first element being shaved off.
                let ptr_start: *mut T = self.raw.ptr().add(new_len).as_ptr().cast();
                // Slice of all shaved off elements.
                let drop_slice = std::slice::from_raw_parts_mut(ptr_start, drop_count);
                std::ptr::drop_in_place(drop_slice);
            }

            // Just update length, we don't need to try to shrink the
            // allocation.
            self.len = new_len;
            return Ok(());
        }

        // Otherwise we do need to reallocate.
        self.raw.resize(new_len)?;
        self.len = new_len;

        Ok(())
    }

    /// Returns a const pointer to the start of this buffer.
    pub fn as_ptr(&self) -> *const T {
        self.raw.ptr().cast().as_ptr()
    }

    /// Returns a mut pointer to the start of this buffer.
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.raw.ptr().cast().as_ptr()
    }

    /// Get a slice reference for the elements in this vec.
    ///
    /// # Safety
    ///
    /// - Reading an element at a given index requires that it's been written to
    ///   previously.
    /// - There should exist no mutable references to anything within this vec's
    ///   allocation (by creating it from a raw pointer).
    pub fn as_slice(&self) -> &[T] {
        debug_assert!(self.len() <= self.raw.capacity());
        let ptr = self.raw.ptr().cast().as_ptr();
        unsafe { std::slice::from_raw_parts(ptr, self.len) }
    }

    /// Get a mutable slice reference for elements in the vec.
    ///
    /// This can be used to initialize the memory after manually setting the
    /// lenth via `set_len`.
    ///
    /// # Safety
    ///
    /// - Reading an element at a given index requires that it's been written to
    ///   previously.
    /// - There should exist no mutable or immutable references to anything
    ///   within this vec's allocation (by creating it from a raw pointer).
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        debug_assert!(self.len() <= self.raw.capacity());
        let ptr = self.raw.ptr().cast().as_ptr();
        unsafe { std::slice::from_raw_parts_mut(ptr, self.len) }
    }

    unsafe fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<T>] {
        debug_assert!(self.len() <= self.raw.capacity());
        let ptr = self.raw.ptr().as_ptr();
        unsafe { std::slice::from_raw_parts_mut(ptr, self.len) }
    }

    /// Returns if this buffer contains the given address.
    ///
    /// This should only be used for verifying pointer arithmetic and not be
    /// part of any core logic.
    ///
    /// Note this will fail for any address if this buffer is zero sized.
    #[allow(unused)]
    pub fn contains_addr(&self, addr: usize) -> bool {
        let min = self.as_ptr().addr();
        let max = min + self.raw.reservation.size();
        addr >= min && addr < max
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
        let align = std::mem::align_of::<T>();
        Self::new_uninit_with_align(manager, cap, align)
    }

    pub fn new_uninit_with_align(
        manager: &impl AsRawBufferManager,
        cap: usize,
        align: usize,
    ) -> Result<Self> {
        let actual_align = std::mem::align_of::<T>();
        if align % actual_align != 0 {
            return Err(DbError::new(
                "Custom alignment needs to be a multiple of actual alignment",
            )
            .with_field("custom", align)
            .with_field("actual", actual_align));
        }

        let manager = manager.as_raw_buffer_manager();

        let size = std::mem::size_of::<T>() * cap;
        let reservation = unsafe { manager.call_reserve(size, align) }?;

        let capacity = if std::mem::size_of::<T>() == 0 {
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
        if new_size <= self.capacity {
            // Don't need to do anything.
            return Ok(());
        }

        let size = std::mem::size_of::<T>() * new_size;
        unsafe { self.manager.call_resize(&mut self.reservation, size)? };
        self.capacity = self.reservation.size() / std::mem::size_of::<T>();
        Ok(())
    }
}

impl<T> Drop for RawDbVec<T> {
    fn drop(&mut self) {
        unsafe { self.manager.call_free_reservation(&mut self.reservation) };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{self, AtomicUsize};

    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;

    #[test]
    fn vec_basic() {
        let mut val = 0;
        let vec = DbVec::<i32>::with_value_init_fn(&DefaultBufferManager, 4, || {
            val += 1;
            val
        })
        .unwrap();

        let s = vec.as_slice();

        assert_eq!(&[1, 2, 3, 4], s);
    }

    #[test]
    fn vec_zero_len() {
        let vec = unsafe { DbVec::<i32>::new_uninit(&DefaultBufferManager, 0).unwrap() };
        let s = vec.as_slice();
        assert!(s.is_empty());
    }

    #[test]
    fn vec_zst() {
        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        struct Zst;

        let vec = DbVec::with_value(&DefaultBufferManager, 4, Zst).unwrap();
        let s = vec.as_slice();

        assert_eq!(&[Zst, Zst, Zst, Zst], s);
    }

    #[test]
    fn vec_drop_elements() {
        struct Droppable {
            count: Arc<AtomicUsize>,
        }

        impl Drop for Droppable {
            fn drop(&mut self) {
                self.count.fetch_add(1, atomic::Ordering::Relaxed);
            }
        }

        let drop_count = Arc::new(AtomicUsize::new(0));

        let vec = DbVec::with_value_init_fn(&DefaultBufferManager, 4, || Droppable {
            count: drop_count.clone(),
        })
        .unwrap();

        std::mem::drop(vec);

        assert_eq!(4, drop_count.load(atomic::Ordering::Relaxed));
    }

    #[test]
    fn vec_drop_uninitialized() {
        // Sanity check.
        //
        // Ensure we can drop uninitialized elements without attempting to
        // access unitialized memory.

        let v = unsafe { DbVec::<i8>::new_uninit(&DefaultBufferManager, 12).unwrap() };
        std::mem::drop(v);
        let v = unsafe { DbVec::<i16>::new_uninit(&DefaultBufferManager, 12).unwrap() };
        std::mem::drop(v);
        let v = unsafe { DbVec::<i32>::new_uninit(&DefaultBufferManager, 12).unwrap() };
        std::mem::drop(v);
        let v = unsafe { DbVec::<i64>::new_uninit(&DefaultBufferManager, 12).unwrap() };
        std::mem::drop(v);
        let v = unsafe { DbVec::<f32>::new_uninit(&DefaultBufferManager, 12).unwrap() };
        std::mem::drop(v);
        let v = unsafe { DbVec::<f64>::new_uninit(&DefaultBufferManager, 12).unwrap() };
        std::mem::drop(v);
    }

    #[test]
    fn vec_resize_grow() {
        let mut vec = DbVec::with_value(&DefaultBufferManager, 4, 3).unwrap();
        unsafe { vec.resize_uninit(6).unwrap() };

        let s = vec.as_slice_mut();
        s[4..].fill(55);

        let s = vec.as_slice();
        assert_eq!(&[3, 3, 3, 3, 55, 55], s);
    }

    #[test]
    fn vec_resize_grow_from_zero() {
        let mut vec = unsafe { DbVec::<i32>::new_uninit(&DefaultBufferManager, 0).unwrap() };
        unsafe { vec.resize_uninit(6).unwrap() };

        let s = vec.as_slice_mut();
        s.fill(4);

        let s = vec.as_slice();
        assert_eq!(&[4, 4, 4, 4, 4, 4], s);
    }

    #[test]
    fn vec_resize_shrink() {
        let mut vec = DbVec::with_value(&DefaultBufferManager, 4, 3).unwrap();
        unsafe { vec.resize_uninit(2).unwrap() };

        let s = vec.as_slice();
        assert_eq!(&[3, 3], s);
    }

    #[test]
    fn vec_resize_shrink_drop_elements() {
        #[derive(Debug)]
        struct Droppable {
            count: Arc<AtomicUsize>,
        }

        impl Drop for Droppable {
            fn drop(&mut self) {
                self.count.fetch_add(1, atomic::Ordering::Relaxed);
            }
        }

        let drop_count = Arc::new(AtomicUsize::new(0));

        let mut vec = DbVec::with_value_init_fn(&DefaultBufferManager, 4, || Droppable {
            count: drop_count.clone(),
        })
        .unwrap();
        unsafe { vec.resize_uninit(1).unwrap() };

        assert_eq!(3, drop_count.load(atomic::Ordering::Relaxed));
    }

    #[test]
    fn vec_distinct_mut_pointers() {
        // Test that we can have multiple pointers that can be written to to the
        // same underlying buffer.
        //
        // This is the basis for how we build the hash join table where we write
        // to multiple non-overlapping pointers as needed.
        //
        // This avoid undefined behavior by:
        //
        // - Ensuring there's no shared slice reference.
        // - Ensuring the pointers don't overlap during writes.

        let mut b = unsafe { DbVec::<i64>::new_uninit(&DefaultBufferManager, 2).unwrap() };

        let p1 = b.as_mut_ptr();
        let p2 = unsafe { b.as_mut_ptr().byte_add(8) };

        unsafe { p1.cast::<i64>().write_unaligned(18) };
        unsafe { p2.cast::<i64>().write_unaligned(1024) };

        let s = b.as_slice();
        assert_eq!(&[18, 1024], s);
    }

    #[test]
    fn vec_new_with_invalid_align() {
        // Align needs to be multiple of 8
        DbVec::<i64>::new_uninit_with_align(&DefaultBufferManager, 2, 12).unwrap_err();
    }

    #[test]
    fn vec_contains_addr() {
        let b = unsafe { DbVec::<i64>::new_uninit(&DefaultBufferManager, 2).unwrap() };

        let addr = b.as_ptr().addr();

        assert!(b.contains_addr(addr));
        assert!(b.contains_addr(addr + 8));
        assert!(!b.contains_addr(addr + 16));
    }

    #[test]
    fn vec_push_slice() {
        let mut vec = DbVec::<i32>::new_from_slice(&DefaultBufferManager, &[1, 2, 3]).unwrap();
        vec.push_slice(&[4, 5, 6]).unwrap();

        let s = vec.as_slice();
        assert_eq!(&[1, 2, 3, 4, 5, 6], s);
    }

    #[test]
    fn vec_push_no_resize() {
        let mut vec = DbVec::<i32>::empty(&DefaultBufferManager);
        assert_eq!(0, vec.capacity());
        vec.push_slice_no_resize(&[1, 2, 3]).unwrap_err();
    }
}
