use std::collections::VecDeque;

use rayexec_error::Result;

use crate::arrays::batch::Batch;

/// Computed batch results from an operator.
#[derive(Debug)]
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
    /// Create a new queue of computed batches.
    ///
    /// This will filter out any batches that have no rows.
    pub fn new<I>(batches: I) -> Self
    where
        I: IntoIterator<Item = Batch>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut iter = batches.into_iter();
        match iter.len() {
            0 => ComputedBatches::None,
            1 => {
                let batch = iter.next().unwrap();
                if batch.num_rows() == 0 {
                    ComputedBatches::None
                } else {
                    ComputedBatches::Single(batch)
                }
            }
            _ => {
                let batches: VecDeque<_> = iter.filter(|b| b.num_rows() > 0).collect();
                if batches.is_empty() {
                    ComputedBatches::None
                } else {
                    ComputedBatches::Multi(batches)
                }
            }
        }
    }

    pub fn total_num_rows(&self) -> usize {
        match self {
            Self::Single(batch) => batch.num_rows(),
            Self::Multi(batches) => batches.iter().map(|b| b.num_rows()).sum(),
            Self::None => 0,
        }
    }

    /// Checks if this collection of batches is empty.
    // TODO: Think about the behavior for this when batches is length zero.
    // There was a bug where loop join produce no batches, called this
    // method, then froze since `is_empty` returns false for empty
    // vecdeques.
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
    pub fn try_pop_front(&mut self) -> Result<Option<Batch>> {
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
