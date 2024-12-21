use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::Result;

pub trait BufferManager: Debug + Clone {
    type Reservation: Reservation;
    type CowPtr<T>: CowPtr<T>;

    /// Reserve some number of bytes.
    fn reserve_external(&self, num_bytes: usize) -> Result<Self::Reservation>;

    fn make_cow<T>(&self, item: T) -> Result<Self::CowPtr<T>>;
}

pub trait Reservation: Debug {
    /// Combine two reservations into a single reservation.
    fn combine(self, other: Self) -> Self;
}

pub trait CowPtr<T>: Clone + AsRef<T> {
    // TODO: Clone on write.
    //
    // Will need to be able to get the underlying reservation in order to track
    // appropriately.
    //
    // Also might need to recurse to make sure everything is writable, not sure
    // yet.
}

impl<T> CowPtr<T> for Arc<T> {}

#[derive(Debug, Clone, Copy)]
pub struct NopBufferManager;

impl BufferManager for NopBufferManager {
    type Reservation = NopReservation;
    type CowPtr<T> = Arc<T>;

    fn reserve_external(&self, _: usize) -> Result<Self::Reservation> {
        Ok(NopReservation)
    }

    fn make_cow<T>(&self, item: T) -> Result<Self::CowPtr<T>> {
        Ok(Arc::new(item))
    }
}

#[derive(Debug)]
pub struct NopReservation;

impl Reservation for NopReservation {
    fn combine(self, _: Self) -> Self {
        self
    }
}
