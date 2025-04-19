use std::alloc::{self, Layout};
use std::ptr::NonNull;

use glaredb_error::{DbError, Result, ResultExt};

use super::buffer_manager::{AsRawBufferManager, RawBufferManager, Reservation};

/// A raw buffer densely allocated on the heap.
///
/// Tracks memory usage through reservations that get released when this buffer
/// gets dropped.
///
/// Note that this is not a general purpose container, and should only be used
/// for storing array data (or raw bytes). Items stored in this buffer **will
/// not** have their `Drop` implementations called.
// TODO: Probably going to change a bit with the array buffer rework
#[derive(Debug)]
pub struct RawBuffer {
    pub(crate) manager: RawBufferManager,
    /// Memory reservation for this buffer.
    ///
    /// Stores the size in bytes of this buffer.
    pub(crate) reservation: Reservation,
    /// Raw pointer to start of vec.
    ///
    /// This stores the pointer as a u8 pointer and it'll be casted to the right
    /// type during array operations.
    pub(crate) ptr: NonNull<u8>,
    /// Capacity for the number of elements (`T`, not bytes) this buffer can
    /// hold.
    ///
    /// This is needed in additional to the reservation to properly handle
    /// zero-sized types (untyped null).
    pub(crate) capacity: usize,
    /// Tracks the alignment of this buffer.
    ///
    /// Used during deallocation without requiring type info.
    pub(crate) align: usize,
}

unsafe impl Send for RawBuffer {}
unsafe impl Sync for RawBuffer {}

impl RawBuffer {
    /// Try to create a new buffer with a given capacity for type `T`.
    ///
    /// This will allocate the underlying buffer to fit exactly `cap` elements.
    /// The block of memory may or may not be initialized. Data must be written
    /// to buffer locations prior to reading from those locations.
    pub fn try_with_capacity<T>(manager: &impl AsRawBufferManager, cap: usize) -> Result<Self> {
        let align = std::mem::align_of::<T>();
        Self::try_with_capacity_and_alignment::<T>(manager, cap, align)
    }

    /// Like `try_with_capacity`, but with manually specified alignment.
    ///
    /// `align` must be:
    ///
    /// - At least 1
    /// - A power of 2
    /// - A multiple of the true alignment of `T`.
    pub fn try_with_capacity_and_alignment<T>(
        manager: &impl AsRawBufferManager,
        cap: usize,
        align: usize,
    ) -> Result<Self> {
        Self::try_with_capacity_and_alignment_inner::<T>(
            manager.as_raw_buffer_manager(),
            cap,
            align,
        )
    }

    fn try_with_capacity_and_alignment_inner<T>(
        manager: RawBufferManager,
        cap: usize,
        align: usize,
    ) -> Result<Self> {
        let true_align = std::mem::align_of::<T>();
        if align % true_align != 0 {
            return Err(DbError::new("Invalid alignment specified")
                .with_field("specified", align)
                .with_field("true_alignment", true_align));
        }

        let size_bytes = std::mem::size_of::<T>() * cap;
        let reservation = unsafe { manager.call_reserve(size_bytes)? };

        let ptr = if size_bytes == 0 {
            // If the amount we're trying to allocate is zero, we still want a
            // valid pointer. A dangling pointer is still well aligned so is
            // usable here.
            //
            // Previously we attempted to init a layout with zero cap to avoid
            // the conditional, but that UB when using the global allocator.
            NonNull::<T>::dangling().cast()
        } else {
            let layout =
                Layout::from_size_align(size_bytes, align).context("failed to create layout")?;

            let ptr = unsafe { alloc::alloc(layout) };
            match NonNull::new(ptr) {
                Some(ptr) => ptr,
                None => alloc::handle_alloc_error(layout),
            }
        };

        Ok(RawBuffer {
            manager,
            reservation,
            ptr,
            capacity: cap,
            align,
        })
    }

    /// Returns the capacity of this buffer in relation to the type this buffer
    /// was initialized with.
    pub const fn typed_capacity(&self) -> usize {
        self.capacity
    }

    /// Returns a const pointer to the start of this buffer.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    /// Returns a mut pointer to the start of this buffer.
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub unsafe fn as_slice<T>(&self) -> &[T] {
        unsafe {
            debug_assert_eq!(0, self.align % std::mem::align_of::<T>());
            debug_assert_eq!(
                0,
                if std::mem::size_of::<T>() > 0 {
                    self.reservation.size() % std::mem::size_of::<T>()
                } else {
                    0
                }
            );
            debug_assert_eq!(
                self.reservation.size(),
                self.capacity * std::mem::size_of::<T>()
            );

            std::slice::from_raw_parts(self.ptr.as_ptr().cast::<T>().cast_const(), self.capacity)
        }
    }

    pub unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        unsafe {
            debug_assert_eq!(0, self.align % std::mem::align_of::<T>());
            debug_assert_eq!(
                0,
                if std::mem::size_of::<T>() > 0 {
                    self.reservation.size() % std::mem::size_of::<T>()
                } else {
                    0
                }
            );
            debug_assert_eq!(
                self.reservation.size(),
                self.capacity * std::mem::size_of::<T>()
            );

            std::slice::from_raw_parts_mut(self.ptr.as_ptr().cast::<T>(), self.capacity)
        }
    }

    /// Reserves memory for holding `additional` number of `T` elements.
    ///
    /// This will reallocate using the buffer manager on the existing memory reservation.
    pub unsafe fn reserve<T>(&mut self, additional: usize) -> Result<()> {
        debug_assert_eq!(0, self.align % std::mem::align_of::<T>());
        debug_assert_eq!(
            0,
            if std::mem::size_of::<T>() > 0 {
                self.reservation.size() % std::mem::size_of::<T>()
            } else {
                0
            }
        );
        debug_assert_eq!(
            self.reservation.size(),
            self.capacity * std::mem::size_of::<T>()
        );

        if additional == 0 {
            // Nothing to do.
            return Ok(());
        }

        if self.capacity == 0 {
            // Just replace self with a new buffer to avoid the below logic (and
            // avoid needing to special case zero-cap layouts).
            *self = Self::try_with_capacity_and_alignment_inner::<T>(
                self.manager,
                additional,
                self.align,
            )?;
            return Ok(());
        }

        if std::mem::size_of::<T>() == 0 {
            // Just need to update capacity.
            self.capacity += additional;
            return Ok(());
        }

        let old_layout = self.current_layout();
        let new_layout = Layout::from_size_align(
            (self.capacity + additional) * std::mem::size_of::<T>(),
            self.align,
        )
        .context("failed to create layout")?;

        let additional_bytes = std::mem::size_of::<T>() * additional;

        // Reserve additional.
        let additional_reservation = unsafe { self.manager.call_reserve(additional_bytes)? };

        let new_ptr = if self.capacity == 0 {
            unsafe { alloc::alloc(new_layout) }
        } else {
            let old_ptr = self.ptr.as_ptr();
            unsafe { alloc::realloc(old_ptr, old_layout, new_layout.size()) }
        };

        self.ptr = match NonNull::new(new_ptr) {
            Some(p) => p,
            None => alloc::handle_alloc_error(new_layout),
        };

        self.capacity += additional;
        self.reservation.merge(additional_reservation);
        debug_assert_eq!(self.reservation.size(), new_layout.size());

        Ok(())
    }

    fn current_layout(&self) -> Layout {
        // If we were able to construct this buffer, then the layout here should
        // always be valid.
        unsafe { Layout::from_size_align_unchecked(self.reservation.size(), self.align) }
    }

    /// Writes all bytes in this buffer to the provided value.
    #[cfg(debug_assertions)]
    #[allow(unused)]
    pub fn debug_fill(&mut self, val: u8) {
        unsafe { self.as_slice_mut::<u8>() }.fill(val);
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
        let max = min + self.reservation.size();
        addr >= min && addr < max
    }
}

impl Drop for RawBuffer {
    fn drop(&mut self) {
        if self.reservation.size() != 0 {
            let layout = self.current_layout();
            unsafe {
                alloc::dealloc(self.ptr.as_ptr(), layout);
            }
        }
        // Always call this as the buffer manager might track number of buffers
        // even if the capacity is zero.
        unsafe { self.manager.call_drop(&self.reservation) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;

    #[test]
    fn new_drop() {
        let b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 4).unwrap();

        assert_eq!(32, b.reservation.size);

        std::mem::drop(b);
    }

    #[test]
    fn new_zero_cap() {
        let b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 0).unwrap();
        assert_eq!(0, b.reservation.size);
        assert_eq!(8, b.align);

        // Ensure we get empty slices.
        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(&[] as &[i64], s);
    }

    #[test]
    fn new_zero_sized_type() {
        #[derive(Debug, PartialEq, Eq)]
        struct Zst;

        let b = RawBuffer::try_with_capacity::<Zst>(&DefaultBufferManager, 4).unwrap();
        assert_eq!(0, b.reservation.size);
        assert_eq!(1, b.align);

        let s = unsafe { b.as_slice::<Zst>() };
        assert_eq!(&[Zst, Zst, Zst, Zst], s);
    }

    #[test]
    fn new_manual_alignment() {
        // Miri helps test this.

        let b = RawBuffer::try_with_capacity_and_alignment::<i64>(&DefaultBufferManager, 4, 32)
            .unwrap();
        assert_eq!(32, b.align);

        let _ = unsafe { b.as_slice::<i64>() };

        let ptr = b.as_ptr();
        assert_eq!(0, ptr.addr() % 32);
    }

    #[test]
    fn as_slice_mut() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 4).unwrap();
        let s = unsafe { b.as_slice_mut::<i64>() };
        assert_eq!(4, s.len());

        for i in 0..4 {
            s[i] = i as i64;
        }

        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(4, s.len());
        assert_eq!(&[0, 1, 2, 3], s);
    }

    #[test]
    fn reserve_additional() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 4).unwrap();
        let s = unsafe { b.as_slice_mut::<i64>() };
        assert_eq!(4, s.len());

        for i in 0..4 {
            s[i] = i as i64;
        }

        unsafe { b.reserve::<i64>(4).unwrap() };
        assert_eq!(64, b.reservation.size);

        let s = unsafe { b.as_slice_mut::<i64>() };
        assert_eq!(8, s.len());

        for i in 0..4 {
            s[i + 4] = s[i] * 2;
        }

        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(&[0, 1, 2, 3, 0, 2, 4, 6], s);
    }

    #[test]
    fn reserve_additional_zero() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 4).unwrap();
        let s = unsafe { b.as_slice_mut::<i64>() };
        for i in 0..4 {
            s[i] = i as i64;
        }

        unsafe { b.reserve::<i64>(0).unwrap() };

        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(&[0, 1, 2, 3], s)
    }

    #[test]
    fn reserve_initial_zero_cap() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 0).unwrap();
        unsafe { b.reserve::<i64>(8).unwrap() };
        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(8, s.len());
    }

    #[test]
    fn reserve_addition_with_zst() {
        #[derive(Debug, PartialEq, Eq)]
        struct Zst;

        let mut b = RawBuffer::try_with_capacity::<Zst>(&DefaultBufferManager, 2).unwrap();
        unsafe { b.reserve::<Zst>(4).unwrap() };

        let s = unsafe { b.as_slice::<Zst>() };
        assert_eq!(&[Zst, Zst, Zst, Zst, Zst, Zst], s);
    }

    #[test]
    fn reserve_keeps_manual_align() {
        // Miri helps test this.

        let mut b = RawBuffer::try_with_capacity_and_alignment::<i64>(&DefaultBufferManager, 4, 32)
            .unwrap();
        unsafe { b.reserve::<i64>(4).unwrap() };
        assert_eq!(32, b.align);

        let ptr = b.as_ptr();
        assert_eq!(0, ptr.addr() % 32);
    }

    #[test]
    fn distinct_mut_pointers() {
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

        let b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 2).unwrap();

        let p1 = b.as_mut_ptr();
        let p2 = unsafe { b.as_mut_ptr().add(8) };

        unsafe { p1.cast::<i64>().write_unaligned(18) };
        unsafe { p2.cast::<i64>().write_unaligned(1024) };

        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(&[18, 1024], s);
    }

    #[test]
    fn contains_addr() {
        let b = RawBuffer::try_with_capacity::<i64>(&DefaultBufferManager, 2).unwrap();

        let addr = b.as_ptr().addr();

        assert!(b.contains_addr(addr));
        assert!(b.contains_addr(addr + 8));
        assert!(!b.contains_addr(addr + 16));
    }
}
