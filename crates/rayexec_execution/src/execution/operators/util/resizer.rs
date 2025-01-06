use std::sync::Arc;

use rayexec_error::Result;

use crate::arrays::batch::Batch2;
use crate::arrays::selection::SelectionVector;
use crate::execution::computed_batch::ComputedBatches;

// TODO: Delete

// TODO: Shouldn't be a const, should be determined when we create the
// executable plans.
pub const DEFAULT_TARGET_BATCH_SIZE: usize = 4096;

/// Resize input batches to produce output batches of a target size.
#[derive(Debug)]
pub struct BatchResizer {
    /// Target batch size.
    target: usize,
    /// Pending input batches.
    pending: Vec<Batch2>,
    /// Current total row count for all batches.
    pending_row_count: usize,
}

impl BatchResizer {
    pub fn new(target_size: usize) -> Self {
        BatchResizer {
            target: target_size,
            pending: Vec::new(),
            pending_row_count: 0,
        }
    }

    /// Try to push a new batch to the resizer, returning possibly resized
    /// batches.
    ///
    /// Typically this will return either no batches or a single batch. However
    /// there is a case where this can return multiple batches if 'len(input) +
    /// pending_row_count > target * 2' (aka very large input batch).
    pub fn try_push(&mut self, batch: Batch2) -> Result<ComputedBatches> {
        if batch.num_rows() == 0 {
            return Ok(ComputedBatches::None);
        }

        if self.pending_row_count + batch.num_rows() == self.target {
            self.pending.push(batch);

            let out = Batch2::concat(&self.pending)?;
            self.pending.clear();
            self.pending_row_count = 0;

            return Ok(ComputedBatches::Single(out));
        }

        if self.pending_row_count + batch.num_rows() > self.target {
            let diff = self.target - self.pending_row_count;

            // Generate selection vectors that logically slice this batch.
            //
            // Batch 'a' will be included in the current set of batches that
            // will concatenated, batch 'b' will initialize the next set.
            let sel_a = SelectionVector::with_range(0..diff);
            let sel_b = SelectionVector::with_range(diff..batch.num_rows());

            let batch_a = batch.select(Arc::new(sel_a));
            let batch_b = batch.select(Arc::new(sel_b));

            self.pending.push(batch_a);

            // Concat current pending + batch a.
            let out = Batch2::concat(&self.pending)?;
            self.pending.clear();
            self.pending_row_count = 0;

            // Now recursively push batch b.
            //
            // This typically produces nothing extra, but may if batch_b is very large.
            match self.try_push(batch_b)? {
                ComputedBatches::Single(out_2) => {
                    // Happens if:
                    //  len(batch_b) > target && len(batch_b) < target * 2
                    return Ok(ComputedBatches::new([out, out_2]));
                }
                ComputedBatches::Multi(mut out_2) => {
                    // Happens if:
                    //  len(batch_b) > target * 2

                    // `out` came first.
                    out_2.push_front(out);
                    return Ok(ComputedBatches::Multi(out_2));
                }
                ComputedBatches::None => {
                    // Simple case, batch_b was less than target.
                    return Ok(ComputedBatches::Single(out));
                }
            }
        }

        // Otherwise just add to pending batches.
        self.pending_row_count += batch.num_rows();
        self.pending.push(batch);

        Ok(ComputedBatches::None)
    }

    pub fn flush_remaining(&mut self) -> Result<ComputedBatches> {
        if self.pending_row_count == 0 {
            return Ok(ComputedBatches::None);
        }

        let out = Batch2::concat(&self.pending)?;
        self.pending.clear();
        self.pending_row_count = 0;
        Ok(ComputedBatches::Single(out))
    }
}
