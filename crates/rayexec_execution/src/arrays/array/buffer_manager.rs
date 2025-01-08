use core::alloc;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use rayexec_error::{Result, ResultExt};

pub trait BufferManager: Debug + Sync + Send + Sized {
    // TODO: T => Spillable or something.
    type CowPtr<T>: CowPtr<T>
    where
        T: Debug;

    fn reserve_external(
        self: &Arc<Self>,
        size_bytes: usize,
        align: usize,
    ) -> Result<Reservation<Self>>;

    fn reserve_from_layout(self: &Arc<Self>, layout: alloc::Layout) -> Result<Reservation<Self>> {
        self.reserve_external(layout.size(), layout.align())
    }

    fn make_cow<T: Debug>(&self, item: T) -> Result<Self::CowPtr<T>, T>;

    /// Drops a memory reservation.
    fn drop_reservation(&self, reservation: &Reservation<Self>);
}

#[derive(Debug)]
pub struct Reservation<B: BufferManager> {
    manager: Arc<B>,
    /// Size in bytes of the memory reservation.
    size: usize,
    /// Alignment of the memory reservation.
    ///
    /// Used during deallocation of raw buffers.
    align: usize,
}

impl<B> Reservation<B>
where
    B: BufferManager,
{
    fn try_new(manager: Arc<B>, size: usize, align: usize) -> Result<Self> {
        alloc::Layout::from_size_align(size, align)
            .context("unable to create layout for reservation")?;

        Ok(Reservation {
            manager,
            size,
            align,
        })
    }

    pub fn manager(&self) -> &Arc<B> {
        &self.manager
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn align(&self) -> usize {
        self.align
    }

    /// Returns an equivalent allocation layout for the reservation.
    ///
    /// Reservations must always produce a valid allocation layout.
    pub fn layout(&self) -> alloc::Layout {
        alloc::Layout::from_size_align(self.size, self.align).unwrap()
    }
}

// TODO: Probably rename, I don't think we want the 'cow' logic on this. Instead
// that'll probably be on ArrayData.
pub trait CowPtr<T>: Debug + Clone + AsRef<T> + Deref<Target = T> {
    // TODO: Clone on write.
    //
    // Will need to be able to get the underlying reservation in order to track
    // appropriately.
    //
    // Also might need to recurse to make sure everything is writable, not sure
    // yet.
}

impl<T> CowPtr<T> for Arc<T> where T: Debug {}

/// Placeholder buffer manager.
#[derive(Debug, Clone)]
pub struct NopBufferManager;

impl BufferManager for NopBufferManager {
    type CowPtr<T>
        = Arc<T>
    where
        T: Debug;

    fn reserve_external(
        self: &Arc<Self>,
        size_bytes: usize,
        align: usize,
    ) -> Result<Reservation<Self>> {
        Reservation::try_new(self.clone(), size_bytes, align)
    }

    fn make_cow<T: Debug>(&self, item: T) -> Result<Self::CowPtr<T>, T> {
        Ok(Arc::new(item))
    }

    fn drop_reservation(&self, _reservation: &Reservation<Self>) {
        // Ok
    }
}
