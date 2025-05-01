use std::cell::OnceCell;

/// Wrapper around a OnceCell that is also Sync if T is Sync.
///
/// # Safety
///
/// Higher level synchronization is required to ensure that setting and getting
/// the value does not happen at the same time.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct UnsafeSyncOnceCell<T>(OnceCell<T>);

impl<T> UnsafeSyncOnceCell<T> {
    pub const fn new() -> Self {
        UnsafeSyncOnceCell(OnceCell::new())
    }

    /// Set the value of the cell.
    ///
    /// Returns Ok(()) if the value was initialized, or Err(value) if the cell
    /// was already initialized.
    ///
    /// # Safety
    ///
    /// `set` must not be called concurrently with other calls to `set` or
    /// `get`.
    pub unsafe fn set(&self, value: T) -> Result<(), T> {
        self.0.set(value)
    }

    /// Get the value of the cell, or None if the cell hasn't been initialized.
    ///
    /// # Safety
    ///
    /// `get` must not be called concurrently with `set`.
    ///
    /// `get` may be called concurrently with other calls to `get`.
    pub unsafe fn get(&self) -> Option<&T> {
        self.0.get()
    }
}

unsafe impl<T: Send> Send for UnsafeSyncOnceCell<T> {}
unsafe impl<T: Sync> Sync for UnsafeSyncOnceCell<T> {}

impl<T> Default for UnsafeSyncOnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_is_sync_send() {
        fn check_sync_send(s: impl Sync + Send) {
            // Nothing, just ensure that it compiles.
        }

        let cell = UnsafeSyncOnceCell::<i32>::new();
        check_sync_send(cell);
    }
}
