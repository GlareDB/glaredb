use std::sync::Arc;

use crate::arrays::batch::Batch;
use crate::arrays::selection::SelectionVector;
use rayexec_error::Result;

use crate::execution::computed_batch::ComputedBatches;

// TODO: Shouldn't be a const, should be determined when we create the
// executable plans.
pub const DEFAULT_TARGET_BATCH_SIZE: usize = 4096;

/// Resize input batches to produce output batches of a target size.
#[derive(Debug)]
pub struct BatchResizer {
    /// Target batch size.
    target: usize,
    /// Pending input batches.
    pending: Vec<Batch>,
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
    pub fn try_push(&mut self, batch: Batch) -> Result<ComputedBatches> {
        if batch.num_rows() == 0 {
            return Ok(ComputedBatches::None);
        }

        if self.pending_row_count + batch.num_rows() == self.target {
            self.pending.push(batch);

            let out = Batch::concat(&self.pending)?;
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
            let out = Batch::concat(&self.pending)?;
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

        let out = Batch::concat(&self.pending)?;
        self.pending.clear();
        self.pending_row_count = 0;
        Ok(ComputedBatches::Single(out))
    }
}

#[cfg(test)]
mod tests {
    use crate::arrays::array::Array;
    use crate::arrays::testutil::assert_batches_eq;

    use super::*;

    #[test]
    fn push_within_target() {
        let batch1 = Batch::try_new([
            Array::from_iter([1, 2, 3]),
            Array::from_iter(["a", "b", "c"]),
        ])
        .unwrap();

        let batch2 = Batch::try_new([
            Array::from_iter([4, 5, 6]),
            Array::from_iter(["d", "e", "f"]),
        ])
        .unwrap();

        let mut resizer = BatchResizer::new(4);

        let out = resizer.try_push(batch1).unwrap();
        assert!(matches!(out, ComputedBatches::None));

        let out = resizer.try_push(batch2).unwrap();
        let got = match out {
            ComputedBatches::Single(batch) => batch,
            other => panic!("unexpected out: {other:?}"),
        };

        let expected = Batch::try_new([
            Array::from_iter([1, 2, 3, 4]),
            Array::from_iter(["a", "b", "c", "d"]),
        ])
        .unwrap();

        assert_batches_eq(&expected, &got);

        let expected_rem =
            Batch::try_new([Array::from_iter([5, 6]), Array::from_iter(["e", "f"])]).unwrap();

        let remaining = match resizer.flush_remaining().unwrap() {
            ComputedBatches::Single(batch) => batch,
            other => panic!("unexpected out: {other:?}"),
        };

        assert_batches_eq(&expected_rem, &remaining);
    }

    #[test]
    fn push_large_batch() {
        // len(batch) > target && len(batch) < target * 2

        let batch = Batch::try_new([
            Array::from_iter([1, 2, 3, 4, 5]),
            Array::from_iter(["a", "b", "c", "d", "e"]),
        ])
        .unwrap();

        let mut resizer = BatchResizer::new(4);
        let got = match resizer.try_push(batch).unwrap() {
            ComputedBatches::Single(batch) => batch,
            other => panic!("unexpected out: {other:?}"),
        };

        let expected = Batch::try_new([
            Array::from_iter([1, 2, 3, 4]),
            Array::from_iter(["a", "b", "c", "d"]),
        ])
        .unwrap();

        assert_batches_eq(&expected, &got);

        let expected_rem =
            Batch::try_new([Array::from_iter([5]), Array::from_iter(["e"])]).unwrap();

        let remaining = match resizer.flush_remaining().unwrap() {
            ComputedBatches::Single(batch) => batch,
            other => panic!("unexpected out: {other:?}"),
        };

        assert_batches_eq(&expected_rem, &remaining);
    }

    #[test]
    fn push_very_large_batch() {
        // len(batch) > target * 2

        let batch = Batch::try_new([
            Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            Array::from_iter(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]),
        ])
        .unwrap();

        let mut resizer = BatchResizer::new(4);
        let gots = match resizer.try_push(batch).unwrap() {
            ComputedBatches::Multi(batches) => batches,
            other => panic!("unexpected out: {other:?}"),
        };

        assert_eq!(2, gots.len());

        let expected1 = Batch::try_new([
            Array::from_iter([1, 2, 3, 4]),
            Array::from_iter(["a", "b", "c", "d"]),
        ])
        .unwrap();
        assert_batches_eq(&expected1, &gots[0]);

        let expected2 = Batch::try_new([
            Array::from_iter([5, 6, 7, 8]),
            Array::from_iter(["e", "f", "g", "h"]),
        ])
        .unwrap();
        assert_batches_eq(&expected2, &gots[1]);

        let expected_rem =
            Batch::try_new([Array::from_iter([9, 10]), Array::from_iter(["i", "j"])]).unwrap();

        let remaining = match resizer.flush_remaining().unwrap() {
            ComputedBatches::Single(batch) => batch,
            other => panic!("unexpected out: {other:?}"),
        };

        assert_batches_eq(&expected_rem, &remaining);
    }

    #[test]
    fn flush_none() {
        let mut resizer = BatchResizer::new(4);
        let out = resizer.flush_remaining().unwrap();
        assert!(matches!(out, ComputedBatches::None));
    }
}
