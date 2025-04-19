use std::fmt::Debug;
use std::ptr::NonNull;

use glaredb_error::Result;

pub trait BufferManager: Debug + Sync + Clone + Send + Sized {
    const VTABLE: &'static BufferManagerVTable = &BufferManagerVTable {
        reserve_fn: |ptr: *const (), num_bytes: usize, align: usize| -> Result<Reservation> {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.reserve(num_bytes, align)
        },
        drop_fn: |ptr: *const (), reservation: &Reservation| {
            let manager = unsafe { ptr.cast::<Self>().as_ref().unwrap() };
            manager.drop_reservation(reservation);
        },
    };

    /// Try to reserve some number of bytes to some alignment.
    ///
    /// Returns a reservation containing the number of bytes allocated, and a
    /// ptr to the start of the allocation.
    ///
    /// This should never error when attempting to reserve zero bytes.
    fn reserve(&self, size_bytes: usize, align: usize) -> Result<Reservation>;

    /// Drops a memory reservation.
    fn drop_reservation(&self, reservation: &Reservation);
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
    vtable: &'static BufferManagerVTable,
}

unsafe impl Sync for RawBufferManager {}
unsafe impl Send for RawBufferManager {}

#[derive(Debug)]
pub struct BufferManagerVTable {
    /// Function called when attempting to reserve some number of bytes.
    reserve_fn: unsafe fn(*const (), usize, usize) -> Result<Reservation>,
    /// Function called when a reservation should be dropped.
    drop_fn: unsafe fn(*const (), reservation: &Reservation),
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

    pub(crate) unsafe fn call_drop(&self, reservation: &Reservation) {
        unsafe { (self.vtable.drop_fn)(self.manager, reservation) }
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
}

impl Reservation {
    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn merge(&mut self, other: Self) {
        self.size += other.size;
    }
}

/// Placeholder buffer manager.
#[derive(Debug, Clone)]
pub struct DefaultBufferManager;

impl BufferManager for DefaultBufferManager {
    fn reserve(&self, size_bytes: usize, align: usize) -> Result<Reservation> {
        unimplemented!()
        // Ok(Reservation { size: size_bytes })
    }

    fn drop_reservation(&self, _reservation: &Reservation) {
        // Ok
    }
}
