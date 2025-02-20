use std::fmt::Debug;

use rayexec_error::Result;

pub trait BufferManager: Debug + Sync + Clone + Send + Sized {
    /// Try to reserve some number of bytes.
    ///
    /// Returns a reservation for keeping tracker of "used" bytes.
    ///
    /// This should never error when attempting to reserve zero bytes.
    fn try_reserve(&self, size_bytes: usize) -> Result<Reservation<Self>>;

    /// Drops a memory reservation.
    fn drop_reservation(&self, reservation: &Reservation<Self>);
}

#[derive(Debug)]
pub struct Reservation<B: BufferManager> {
    manager: B,
    /// Size in bytes of the memory reservation.
    size: usize,
}

impl<B> Reservation<B>
where
    B: BufferManager,
{
    pub fn merge(&mut self, other: Self) {
        self.size += other.size;
    }

    pub const fn manager(&self) -> &B {
        &self.manager
    }

    pub const fn size(&self) -> usize {
        self.size
    }
}

/// Placeholder buffer manager.
#[derive(Debug, Clone)]
pub struct NopBufferManager;

impl BufferManager for NopBufferManager {
    fn try_reserve(&self, size_bytes: usize) -> Result<Reservation<Self>> {
        Ok(Reservation {
            manager: self.clone(),
            size: size_bytes,
        })
    }

    fn drop_reservation(&self, _reservation: &Reservation<Self>) {
        // Ok
    }
}
