use std::collections::VecDeque;

use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

/// Computed batch results from an operator.
#[derive(Debug, Clone, PartialEq)] // TODO: Remove clone.
pub enum ComputedBatches {
    /// A single batch was computed.
    Single(Batch),
    /// Multiple batches were computed.
    ///
    /// These should be ordered by which batch should be pushed to next operator
    /// first.
    Multi(VecDeque<Batch>),
    /// No batches computed.
    None,
    // TODO: Spill references
}

impl ComputedBatches {
    pub fn new_multi<B>(batches: impl IntoIterator<Item = B>) -> Self
    where
        B: Into<Batch>,
    {
        Self::Multi(batches.into_iter().map(|b| b.into()).collect())
    }

    /// Checks if this collection of batches is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Multi(batches) => batches.is_empty(),
            Self::None => true,
            Self::Single(_) => false,
        }
    }

    /// Checks if this collection of batches actually contains a batch.
    pub fn has_batches(&self) -> bool {
        !self.is_empty()
    }

    /// Takes the current collection of batches, and replaces it with None.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, ComputedBatches::None)
    }

    /// Tries to get the next batch from this collection, returning None when no
    /// batches remain.
    pub fn try_next(&mut self) -> Result<Option<Batch>> {
        match self {
            Self::Single(_) => {
                let orig = std::mem::replace(self, Self::None);
                let batch = match orig {
                    Self::Single(batch) => batch,
                    _ => unreachable!(),
                };

                Ok(Some(batch))
            }
            Self::Multi(batches) => Ok(batches.pop_front()),
            Self::None => Ok(None),
        }
    }
}

impl From<Batch> for ComputedBatches {
    fn from(value: Batch) -> Self {
        Self::Single(value)
    }
}
