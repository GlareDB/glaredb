#![allow(dead_code)]

use std::alloc::{self, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

use rayexec_error::{Result, ResultExt};

use super::buffer_manager::{BufferManager, Reservation};

#[derive(Debug)]
pub struct RawBuffer<B: BufferManager> {
    /// Memory reservation for this buffer.
    pub(crate) reservation: Reservation<B>,
    /// Raw pointer to start of vec.
    ///
    /// This stores the pointer as a u8 pointer and it'll be casted to the right
    /// type during array operations.
    pub(crate) ptr: NonNull<u8>,
}

unsafe impl<B: BufferManager> Send for RawBuffer<B> {}
unsafe impl<B: BufferManager> Sync for RawBuffer<B> {}

impl<B> RawBuffer<B>
where
    B: BufferManager,
{
    /// Try to create a new buffer with a given capacity for type `T`.
    pub fn try_with_capacity<T>(manager: &Arc<B>, cap: usize) -> Result<Self> {
        assert!(std::mem::size_of::<T>() > 0);

        if cap == 0 {
            return Ok(RawBuffer {
                reservation: manager.reserve_external(0, 0)?,
                ptr: NonNull::dangling(),
            });
        }

        let layout = Layout::array::<T>(cap).context("failed to create layout")?;
        let ptr = unsafe { alloc::alloc(layout) };
        let ptr = match NonNull::new(ptr) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        let reservation = manager.reserve_from_layout(layout)?;

        Ok(RawBuffer { reservation, ptr })
    }

    pub unsafe fn as_slice<T>(&self) -> &[T] {
        debug_assert_eq!(std::mem::align_of::<T>(), self.reservation.align());
        debug_assert_eq!(0, self.reservation.size() % std::mem::size_of::<T>());

        let cap = self.reservation.size() / std::mem::size_of::<T>();
        std::slice::from_raw_parts(self.ptr.as_ptr().cast::<T>().cast_const(), cap)
    }

    pub unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        debug_assert_eq!(std::mem::align_of::<T>(), self.reservation.align());
        debug_assert_eq!(0, self.reservation.size() % std::mem::size_of::<T>());

        let cap = self.reservation.size() / std::mem::size_of::<T>();
        std::slice::from_raw_parts_mut(self.ptr.as_ptr().cast::<T>(), cap)
    }

    pub unsafe fn reserve<T>(&mut self, manager: &Arc<B>, additional: usize) -> Result<()> {
        debug_assert_eq!(std::mem::align_of::<T>(), self.reservation.align());
        debug_assert_eq!(0, self.reservation.size() % std::mem::size_of::<T>());

        let cap = self.reservation.size() / std::mem::size_of::<T>();
        let layout = Layout::array::<T>(cap + additional).context("failed to create layout")?;

        let new_ptr = if cap == 0 {
            unsafe { alloc::alloc(layout) }
        } else {
            let old_ptr = self.ptr.as_ptr();
            let old_layout = self.reservation.layout();
            unsafe { alloc::realloc(old_ptr, old_layout, layout.size()) }
        };

        self.ptr = match NonNull::new(new_ptr) {
            Some(p) => p,
            None => alloc::handle_alloc_error(layout),
        };

        self.reservation = manager.reserve_from_layout(layout)?;

        Ok(())
    }
}

impl<B> Drop for RawBuffer<B>
where
    B: BufferManager,
{
    fn drop(&mut self) {
        if self.reservation.size() != 0 {
            let layout = self.reservation.layout();
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
        let b = RawBuffer::try_with_capacity::<i64>(&Arc::new(NopBufferManager), 4).unwrap();

        assert_eq!(32, b.reservation.size());

        std::mem::drop(b);
    }

    #[test]
    fn as_slice_mut() {
        let mut b = RawBuffer::try_with_capacity::<i64>(&Arc::new(NopBufferManager), 4).unwrap();
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
        let mut b = RawBuffer::try_with_capacity::<i64>(&Arc::new(NopBufferManager), 4).unwrap();
        let s = unsafe { b.as_slice_mut::<i64>() };
        assert_eq!(4, s.len());

        for i in 0..4 {
            s[i] = i as i64;
        }

        unsafe { b.reserve::<i64>(&Arc::new(NopBufferManager), 4).unwrap() };
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
