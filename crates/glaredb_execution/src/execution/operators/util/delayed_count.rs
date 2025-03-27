use glaredb_error::{DbError, Result};

use crate::config::session::Partitions;

/// A count that can only be initialized once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayedPartitionCount(Option<u16>);

impl DelayedPartitionCount {
    /// Creates a new uninitialized count.
    pub const fn uninit() -> Self {
        DelayedPartitionCount(None)
    }

    pub const fn is_uninit(&self) -> bool {
        self.0.is_none()
    }

    /// Sets this count to the provided value.
    ///
    /// Errors if:
    /// - The count was previously set.
    /// - The provided falls outside the min/max partition range.
    pub fn set(&mut self, count: usize) -> Result<()> {
        if self.0.is_some() {
            return Err(DbError::new("Delayed count has already been set"));
        }
        Partitions::validate_value(count)?;

        self.0 = Some(count as u16);
        Ok(())
    }

    /// Get the current count.
    ///
    /// Errors if the count hasn't been initialized yet.
    pub fn current(&self) -> Result<usize> {
        match self.0 {
            Some(count) => Ok(count as usize),
            None => Err(DbError::new(
                "Attempted to get current count from an unitialized count",
            )),
        }
    }

    /// Decrement the count by one, returning the new value.
    ///
    /// Errors if the current count is zero, or if the count hasn't been
    /// initialized.
    pub fn dec_by_one(&mut self) -> Result<usize> {
        match self.0.as_mut() {
            Some(count) => {
                if *count == 0 {
                    return Err(DbError::new("Attempted to decrement 0"));
                }
                *count -= 1;
                Ok(*count as usize)
            }
            None => Err(DbError::new(
                "Attempted to decrement an unitialized delayed count",
            )),
        }
    }
}
