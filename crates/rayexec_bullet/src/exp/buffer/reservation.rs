use std::fmt::Debug;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;

use rayexec_error::Result;

pub trait ReservationTracker: Debug + Clone {
    type Reservation: Reservation;

    /// Reserve `additional` number of bytes.
    fn reserve(&self, additional: usize) -> Result<Self::Reservation>;
}

pub trait Reservation: Debug {
    /// Combine two reservations into a single reservation.
    fn combine(self, other: Self) -> Self;

    /// "Free" this reservation.
    ///
    /// This should only be called once.
    fn free(&mut self);
}

#[derive(Debug, Clone, Copy)]
pub struct NopReservationTracker;

impl ReservationTracker for NopReservationTracker {
    type Reservation = NopReservation;
    fn reserve(&self, _additional: usize) -> Result<Self::Reservation> {
        Ok(NopReservation)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NopReservation;

impl Reservation for NopReservation {
    fn combine(self, other: Self) -> Self {
        self
    }

    fn free(&mut self) {}
}

#[derive(Debug, Clone, Default)]
pub struct AtomicReservationTracker {
    total: Arc<AtomicUsize>,
}

impl AtomicReservationTracker {
    pub fn total_reserved(&self) -> usize {
        self.total.load(atomic::Ordering::Relaxed)
    }
}

impl ReservationTracker for AtomicReservationTracker {
    type Reservation = AtomicReservation;

    fn reserve(&self, additional: usize) -> Result<Self::Reservation> {
        self.total.fetch_add(additional, atomic::Ordering::Relaxed);
        Ok(AtomicReservation {
            freed: false,
            reserved: additional,
            total: self.total.clone(),
        })
    }
}

// TODO: A bit large
#[derive(Debug)]
pub struct AtomicReservation {
    freed: bool,
    reserved: usize,
    total: Arc<AtomicUsize>,
}

impl Reservation for AtomicReservation {
    fn combine(mut self, other: Self) -> Self {
        self.reserved += other.reserved;
        self
    }

    fn free(&mut self) {
        assert!(!self.freed);
        self.total
            .fetch_sub(self.reserved, atomic::Ordering::Relaxed);
        self.freed = true
    }
}
