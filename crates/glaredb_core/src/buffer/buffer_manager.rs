use std::alloc::{self, Layout};
use std::fmt::Debug;
use std::ptr::NonNull;

use glaredb_error::{DbError, Result, ResultExt};

pub trait BufferManager: Debug + Sync + Clone + Send + Sized {
    /// Try to allocate some number of bytes to some alignment.
    ///
    /// Returns a reservation containing the number of bytes allocated, and a
    /// ptr to the start of the allocation.
    ///
    /// This should never error when attempting to reserve zero bytes.
    fn allocate(&self, size: usize, align: usize) -> Result<Reservation>;

    /// Resizes a reservation.
    ///
    /// The pointer on the reservation will be updated.
    fn resize(&self, reservation: &mut Reservation, new_size: usize) -> Result<()>;

    /// Deallocate a memory reservation.
    fn deallocate(&self, reservation: &mut Reservation);
}

trait BufferManagerVTable: BufferManager {
    const VTABLE: &'static RawBufferManagerVTable;
}

impl<B> BufferManagerVTable for B
where
    B: BufferManager,
{
    const VTABLE: &'static RawBufferManagerVTable = &RawBufferManagerVTable {
        allocate_fn: |ptr: *const (), size: usize, align: usize| -> Result<Reservation> {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.allocate(size, align)
        },
        resize_fn: |ptr, reservation, new_size| -> Result<()> {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.resize(reservation, new_size)
        },
        deallocate_fn: |ptr: *const (), reservation: &mut Reservation| {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.deallocate(reservation);
        },
    };
}

pub trait AsRawBufferManager {
    fn as_raw_buffer_manager(&self) -> RawBufferManager;
}

impl<B> AsRawBufferManager for B
where
    B: BufferManager,
{
    fn as_raw_buffer_manager(&self) -> RawBufferManager {
        RawBufferManager::from_buffer_manager(self)
    }
}

impl AsRawBufferManager for RawBufferManager {
    fn as_raw_buffer_manager(&self) -> RawBufferManager {
        *self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RawBufferManager {
    manager: *const (),
    vtable: &'static RawBufferManagerVTable,
}

unsafe impl Sync for RawBufferManager {}
unsafe impl Send for RawBufferManager {}

#[derive(Debug)]
pub struct RawBufferManagerVTable {
    /// Function called when attempting to reserve some number of bytes.
    allocate_fn: unsafe fn(*const (), usize, usize) -> Result<Reservation>,
    /// Function when resizing a reservation.
    resize_fn: unsafe fn(*const (), &mut Reservation, usize) -> Result<()>,
    /// Function called when a reservation should be freed.
    deallocate_fn: unsafe fn(*const (), reservation: &mut Reservation),
}

impl RawBufferManager {
    // TODO: Lifetime constraints.
    pub(crate) fn from_buffer_manager<B>(manager: &B) -> Self
    where
        B: BufferManager,
    {
        let ptr = (manager as *const B).cast::<()>();
        RawBufferManager {
            manager: ptr,
            vtable: B::VTABLE,
        }
    }

    pub(crate) unsafe fn call_allocate(&self, size: usize, align: usize) -> Result<Reservation> {
        unsafe { (self.vtable.allocate_fn)(self.manager, size, align) }
    }

    pub(crate) unsafe fn call_resize(
        &self,
        reservation: &mut Reservation,
        new_size: usize,
    ) -> Result<()> {
        unsafe { (self.vtable.resize_fn)(self.manager, reservation, new_size) }
    }

    pub(crate) unsafe fn call_deallocate(&self, reservation: &mut Reservation) {
        unsafe { (self.vtable.deallocate_fn)(self.manager, reservation) }
    }
}

#[derive(Debug)]
pub struct Reservation {
    /// Pointer to the start of the memory reservation.
    ///
    /// This pointer must always be well-aligned even for a size of zero.
    ptr: NonNull<u8>,
    /// Size in bytes of the memory reservation.
    size: usize,
    /// Alignment of the reservation.
    align: usize,
}

impl Reservation {
    pub const fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub const fn size(&self) -> usize {
        self.size
    }

    pub const fn align(&self) -> usize {
        self.align
    }
}

/// Default buffer manager that just freely allocates everything.
#[derive(Debug, Clone)]
pub struct DefaultBufferManager;

impl BufferManager for DefaultBufferManager {
    fn allocate(&self, size: usize, align: usize) -> Result<Reservation> {
        if align == 0 {
            return Err(DbError::new("Cannot have zero alignment"));
        }

        // If we're trying to allocate zero bytes, we need avoid hitting the
        // global allocator since that's UB.
        if size == 0 {
            // Create well-aligned dangling pointer. It's important we do this
            // since a vec containing zero values needs to still be aligned.
            let dangling = std::ptr::without_provenance::<u8>(align);
            // SAFETY: We create a pointer using `align` as the address, and we
            // already checked that `align` is non-zero.
            let dangling = unsafe { NonNull::new_unchecked(dangling.cast_mut()) };

            // Now just return the empty reservation with the dangling pointer.
            return Ok(Reservation {
                ptr: dangling.cast(),
                size,
                align,
            });
        }

        let layout =
            Layout::from_size_align(size, align).context("failed to create memory layout")?;
        // SAFETY: Alloc returns a pointer suitably aligned or null.
        let ptr = unsafe { alloc::alloc(layout) };
        let ptr = match NonNull::new(ptr) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        // TODO: Possibly a pointer map? Would require global state, which we
        // need anyways for spilling.

        Ok(Reservation { ptr, size, align })
    }

    fn resize(&self, reservation: &mut Reservation, new_size: usize) -> Result<()> {
        if reservation.size() == new_size {
            // Nothing to do.
            return Ok(());
        }

        if reservation.size() == 0 {
            // We have nothing to reallocate, and we have a dangling pointer.
            // Just create a new reservation.
            *reservation = self.allocate(new_size, reservation.align())?;
            return Ok(());
        }

        let old_layout = Layout::from_size_align(reservation.size, reservation.align)
            .context("failed to create current memory layout")?;

        let new_ptr = unsafe { alloc::realloc(reservation.ptr.as_ptr(), old_layout, new_size) };
        let new_ptr = match NonNull::new(new_ptr) {
            Some(ptr) => ptr,
            None => return Err(DbError::new("null pointer")),
        };

        reservation.ptr = new_ptr;
        reservation.size = new_size;

        Ok(())
    }

    fn deallocate(&self, reservation: &mut Reservation) {
        if reservation.size() == 0 {
            // Nothing to deallocate, and our pointer is dangling.
            return;
        }

        // SAFETY: Matches the same Layout we used to alloc.
        let layout = Layout::from_size_align(reservation.size, reservation.align).unwrap();
        unsafe { alloc::dealloc(reservation.ptr.as_ptr(), layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_reserve_basic() {
        let mut reservation = DefaultBufferManager.allocate(4, 2).unwrap();
        assert_eq!(4, reservation.size());
        assert_eq!(2, reservation.align());
        DefaultBufferManager.deallocate(&mut reservation);
    }

    #[test]
    fn default_reserve_zero_size() {
        let mut reservation = DefaultBufferManager.allocate(0, 2).unwrap();
        assert_eq!(0, reservation.size());
        assert_eq!(2, reservation.align());
        DefaultBufferManager.deallocate(&mut reservation);
    }

    #[test]
    fn default_reserve_zero_align_error() {
        DefaultBufferManager.allocate(4, 0).unwrap_err();
    }

    #[test]
    fn default_resize_grow() {
        let mut reservation = DefaultBufferManager.allocate(4, 2).unwrap();
        DefaultBufferManager.resize(&mut reservation, 8).unwrap();
        assert_eq!(8, reservation.size());
        assert_eq!(2, reservation.align());
        DefaultBufferManager.deallocate(&mut reservation);
    }

    #[test]
    fn default_resize_shrink() {
        let mut reservation = DefaultBufferManager.allocate(4, 2).unwrap();
        DefaultBufferManager.resize(&mut reservation, 2).unwrap();
        assert_eq!(2, reservation.size());
        assert_eq!(2, reservation.align());
        DefaultBufferManager.deallocate(&mut reservation);
    }

    #[test]
    fn default_resize_grow_from_zero() {
        let mut reservation = DefaultBufferManager.allocate(0, 2).unwrap();
        DefaultBufferManager.resize(&mut reservation, 8).unwrap();
        assert_eq!(8, reservation.size());
        assert_eq!(2, reservation.align());
        DefaultBufferManager.deallocate(&mut reservation);
    }

    #[test]
    fn default_resize_same_size() {
        let mut reservation = DefaultBufferManager.allocate(4, 2).unwrap();
        DefaultBufferManager.resize(&mut reservation, 4).unwrap();
        assert_eq!(4, reservation.size());
        assert_eq!(2, reservation.align());
        DefaultBufferManager.deallocate(&mut reservation);
    }
}
