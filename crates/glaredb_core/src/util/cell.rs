use std::cell::UnsafeCell;

/// Wrapper around a OnceCell that is also Sync if T is Sync.
///
/// # Safety
///
/// Higher level synchronization is required to ensure that setting and getting
/// the value does not happen at the same time.
#[derive(Debug)]
#[repr(transparent)]
pub struct UnsafeSyncOnceCell<T>(UnsafeCell<Option<T>>);

impl<T> UnsafeSyncOnceCell<T> {
    pub const fn new() -> Self {
        UnsafeSyncOnceCell(UnsafeCell::new(None))
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
        // Adapted from std::cell::OnceCell
        unsafe {
            if self.get().is_some() {
                return Err(value);
            }

            // SAFETY: This is the only place where we set the slot, no races
            // due to reentrancy/concurrency are possible, and we've checked
            // that slot is currently `None`, so this write maintains the
            // `inner`'s invariant.
            let slot = &mut *self.0.get();
            let _ = slot.insert(value);

            Ok(())
        }
    }

    /// Get the value of the cell, or None if the cell hasn't been initialized.
    ///
    /// # Safety
    ///
    /// `get` must not be called concurrently with `set`.
    ///
    /// `get` may be called concurrently with other calls to `get`.
    pub unsafe fn get(&self) -> Option<&T> {
        unsafe { &*self.0.get() }.as_ref()
    }

    /// Get a mutable reference to the cell, or None if the cell hasn't been
    /// initialized.
    ///
    /// # Safety
    ///
    /// There must not be any active references to the inner cell value.
    ///
    /// Cannot be called concurrently with pretty much all other methods.
    pub unsafe fn get_mut(&self) -> Option<&mut T> {
        unsafe { &mut *self.0.get() }.as_mut()
    }

    /// Take the inner value, or return None if the cell hasn't been
    /// initialized.
    ///
    /// # Safety
    ///
    /// There must not be any active references to the inner cell value.
    ///
    /// Cannot be called concurrently with pretty much all other methods.
    pub unsafe fn take(&self) -> Option<T> {
        unsafe { &mut *self.0.get() }.take()
    }
}

unsafe impl<T: Send> Send for UnsafeSyncOnceCell<T> {}
unsafe impl<T: Sync> Sync for UnsafeSyncOnceCell<T> {}

impl<T> Default for UnsafeSyncOnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// An unsafe cell that implements `Sync` if `T` is `Sync`.
///
/// See the nightly only `SyncUnsafeCell` in std::cell.
///
/// # Safety
///
/// Higher level synchronization is needed to ensure mutables accessed do not
/// happen concurrently with each other or any other accesses.
#[derive(Debug)]
#[repr(transparent)]
pub struct UnsafeSyncCell<T>(UnsafeCell<T>);

impl<T> UnsafeSyncCell<T> {
    pub const fn new(val: T) -> Self {
        UnsafeSyncCell(UnsafeCell::new(val))
    }

    /// Get a reference to the underlying object.
    ///
    /// # Safety
    ///
    /// Must ensure that there's no outstanding mutable references to the
    /// object.
    ///
    /// This is safe to call concurrentl with other calls to `get`.
    pub unsafe fn get(&self) -> &T {
        // SAFETY: The pointer deref is safe since we initialized this object
        // with a non-null value.
        unsafe { &*self.0.get() }
    }

    /// Get a mutable reference to the underlying object.
    ///
    /// # Safety
    ///
    /// Must ensure there's no existing references to the underlying object.
    ///
    /// Cannot be called concurrently with other calls to `get_mut` (or `get`).
    ///
    /// The clippy lint is valid. The above must be upheld.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_mut(&self) -> &mut T {
        // SAFETY: Same as `get`.
        unsafe { &mut *self.0.get() }
    }
}

unsafe impl<T: Send> Send for UnsafeSyncCell<T> {}
unsafe impl<T: Sync> Sync for UnsafeSyncCell<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_sync_send(_s: impl Sync + Send) {
        // Nothing, just ensure that it compiles.
    }

    #[test]
    fn once_cell_is_sync_send() {
        let cell = UnsafeSyncOnceCell::<i32>::new();
        check_sync_send(cell);
    }

    #[test]
    fn cell_is_sync_send() {
        let cell = UnsafeSyncCell::<i32>::new(4);
        check_sync_send(cell);
    }
}
