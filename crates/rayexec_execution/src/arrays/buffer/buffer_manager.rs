use std::fmt::Debug;

use rayexec_error::Result;

pub trait BufferManager: Debug + Sync + Send + Clone {
    type Reservation: Debug;

    fn reserve_external(&self, additional_bytes: usize) -> Result<Self::Reservation>;
}

/// Placeholder buffer manager.
#[derive(Debug, Clone)]
pub struct NopBufferManager;

impl BufferManager for NopBufferManager {
    type Reservation = ();

    fn reserve_external(&self, _additional_bytes: usize) -> Result<Self::Reservation> {
        Ok(())
    }
}
