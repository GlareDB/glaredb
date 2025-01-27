use std::alloc::{self, Layout};
use std::ptr::NonNull;

use rayexec_error::{Result, ResultExt};
use stdutil::marker::PhantomCovariant;

use super::buffer_manager::{BufferManager, Reservation};

#[derive(Debug)]
pub struct TypedRawBuffer<T, B: BufferManager> {
    pub(crate) _type: PhantomCovariant<T>,
    pub(crate) raw: RawBuffer<B>,
}

impl<T, B> TypedRawBuffer<T, B>
where
    B: BufferManager,
{
    pub fn try_with_capacity(manager: &B, cap: usize) -> Result<Self> {
        let raw = RawBuffer::try_with_capacity::<T>(manager, cap)?;
        Ok(TypedRawBuffer {
            _type: PhantomCovariant::new(),
            raw,
        })
    }

    pub fn reserve(&mut self, additional: usize) -> Result<()> {
        unsafe { self.raw.reserve::<T>(additional) }
    }

    pub fn capacity(&self) -> usize {
        self.raw.capacity()
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { self.raw.as_slice::<T>() }
    }

    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { self.raw.as_slice_mut() }
    }
}

#[derive(Debug)]
pub struct RawBuffer<B: BufferManager> {
    /// Memory reservation for this buffer.
    pub(crate) reservation: Reservation<B>,
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

unsafe impl<B: BufferManager> Send for RawBuffer<B> {}
unsafe impl<B: BufferManager> Sync for RawBuffer<B> {}

impl<B> RawBuffer<B>
where
    B: BufferManager,
{
    /// Try to create a new buffer with a given capacity for type `T`.
    pub fn try_with_capacity<T>(manager: &B, cap: usize) -> Result<Self> {
        let align = std::mem::align_of::<T>();
        let size_bytes = std::mem::size_of::<T>() * cap;

        let reservation = manager.try_reserve(size_bytes)?;

        // Note that creating zero cap layouts is fine. We'll end up with a
        // valid pointer that can be cast to an empty slice.
        let layout = Layout::array::<T>(cap).context("failed to create layout")?;
        let ptr = unsafe { alloc::alloc(layout) };
        let ptr = match NonNull::new(ptr) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        Ok(RawBuffer {
            reservation,
            ptr,
            capacity: cap,
            align,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub unsafe fn as_slice<T>(&self) -> &[T] {
        debug_assert_eq!(std::mem::align_of::<T>(), self.align);
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

    pub unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        debug_assert_eq!(std::mem::align_of::<T>(), self.align);
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

    /// Reserves memory for holding `additional` number of `T` elements.
    ///
    /// This will reallocate using the buffer manager on the existing memory reservation.
    pub unsafe fn reserve<T>(&mut self, additional: usize) -> Result<()> {
        debug_assert_eq!(std::mem::align_of::<T>(), self.align);
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

        let cap = self.reservation.size() / std::mem::size_of::<T>();

        let old_layout = self.layout();
        let new_layout =
            Layout::array::<T>(cap + additional).context("failed to create new layout")?;

        let additional_bytes = std::mem::size_of::<T>() * additional;

        // Reserve additional.
        let additional_reservation = self.reservation.manager().try_reserve(additional_bytes)?;

        let new_ptr = if cap == 0 {
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

    const fn layout(&self) -> Layout {
        unsafe { Layout::from_size_align_unchecked(self.reservation.size(), self.align) }
    }
}

impl<B> Drop for RawBuffer<B>
where
    B: BufferManager,
{
    fn drop(&mut self) {
        if self.reservation.size() != 0 {
            let layout = self.layout();
            unsafe {
                alloc::dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;

    #[test]
    fn new_drop() {
        let b = RawBuffer::try_with_capacity::<i64>(&NopBufferManager, 4).unwrap();

        assert_eq!(32, b.reservation.size());

        std::mem::drop(b);
    }

    #[test]
    fn new_zero_cap() {
        let b = RawBuffer::try_with_capacity::<i64>(&NopBufferManager, 0).unwrap();
        assert_eq!(0, b.reservation.size());
        assert_eq!(8, b.align);

        // Ensure we get empty slices.
        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(&[] as &[i64], s);
    }

    #[test]
    fn as_slice_mut() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&NopBufferManager, 4).unwrap();
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
    fn reserve() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&NopBufferManager, 4).unwrap();
        let s = unsafe { b.as_slice_mut::<i64>() };
        assert_eq!(4, s.len());

        for i in 0..4 {
            s[i] = i as i64;
        }

        unsafe { b.reserve::<i64>(4).unwrap() };
        assert_eq!(64, b.reservation.size());

        let s = unsafe { b.as_slice_mut::<i64>() };
        assert_eq!(8, s.len());

        for i in 0..4 {
            s[i + 4] = s[i] * 2;
        }

        let s = unsafe { b.as_slice::<i64>() };
        assert_eq!(&[0, 1, 2, 3, 0, 2, 4, 6], s);
    }
}
