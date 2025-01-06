use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use rayexec_error::Result;

pub trait BufferManager: Debug + Sync + Send + Clone {
    type Reservation: Debug;
    // TODO: T => Spillable or something.
    type CowPtr<T>: CowPtr<T>
    where
        T: Debug;

    fn reserve_external(&self, additional_bytes: usize) -> Result<Self::Reservation>;

    fn make_cow<T: Debug>(&self, item: T) -> Result<Self::CowPtr<T>, T>;
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
    type Reservation = ();
    type CowPtr<T>
        = Arc<T>
    where
        T: Debug;

    fn reserve_external(&self, _additional_bytes: usize) -> Result<Self::Reservation> {
        Ok(())
    }

    fn make_cow<T: Debug>(&self, item: T) -> Result<Self::CowPtr<T>, T> {
        Ok(Arc::new(item))
    }
}
