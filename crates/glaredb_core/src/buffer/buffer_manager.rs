use std::alloc::{self, Layout};
use std::fmt::Debug;
use std::ptr::NonNull;

use glaredb_error::{DbError, Result, ResultExt};

pub trait BufferManager: Debug + Sync + Clone + Send + Sized {
    /// Try to reserve some number of bytes to some alignment.
    ///
    /// Returns a reservation containing the number of bytes allocated, and a
    /// ptr to the start of the allocation.
    ///
    /// This should never error when attempting to reserve zero bytes.
    fn reserve(&self, size: usize, align: usize) -> Result<Reservation>;

    /// Resizes a reservation.
    ///
    /// The pointer on the reservation will be updated.
    fn resize(&self, reservation: &mut Reservation, new_size: usize) -> Result<()>;

    /// Drops a memory reservation.
    fn free_reservation(&self, reservation: &mut Reservation);
}

trait BufferManagerVTable: BufferManager {
    const VTABLE: &'static RawBufferManagerVTable;
}

impl<B> BufferManagerVTable for B
where
    B: BufferManager,
{
    const VTABLE: &'static RawBufferManagerVTable = &RawBufferManagerVTable {
        reserve_fn: |ptr: *const (), size: usize, align: usize| -> Result<Reservation> {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.reserve(size, align)
        },
        free_reservation_fn: |ptr: *const (), reservation: &mut Reservation| {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.free_reservation(reservation);
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
    reserve_fn: unsafe fn(*const (), usize, usize) -> Result<Reservation>,
    /// Function called when a reservation should be freed.
    free_reservation_fn: unsafe fn(*const (), reservation: &mut Reservation),
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

    pub(crate) unsafe fn call_free_reservation(&self, reservation: &mut Reservation) {
        unsafe { (self.vtable.free_reservation_fn)(self.manager, reservation) }
    }

    pub(crate) unsafe fn call_reserve(
        &self,
        num_bytes: usize,
        align: usize,
    ) -> Result<Reservation> {
        unsafe { (self.vtable.reserve_fn)(self.manager, num_bytes, align) }
    }
}

#[derive(Debug)]
pub struct Reservation {
    /// Pointer to the start of the memory reservation.
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
    fn reserve(&self, size: usize, align: usize) -> Result<Reservation> {
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

    fn free_reservation(&self, reservation: &mut Reservation) {
        // SAFETY: Matches the same Layout we used to alloc.
        let layout = Layout::from_size_align(reservation.size, reservation.align).unwrap();
        unsafe { alloc::dealloc(reservation.ptr.as_ptr(), layout) };
    }
}
